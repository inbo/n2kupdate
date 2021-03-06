#' Store model type in the database
#' @param model_type a data.frame with the modeltypes. Must have a variable "description". The variable "long_description" is optional.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate select arrange
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable
store_model_type <- function(model_type, hash, clean = TRUE, conn){
  model_type <- character_df(model_type)
  assert_that(has_name(model_type, "description"))
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))
  if (missing(hash)) {
    hash <- sha1(list(model_type, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }


  if (has_name(model_type, "long_description")) {
    model_type <- model_type %>%
      select(.data$description, .data$long_description)
  } else {
    model_type <- model_type %>%
      select(.data$description) %>%
      mutate(long_description = NA_character_)
  }

  if (clean) {
    dbBegin(conn)
  }

  staging <- model_type %>%
    mutate(id = NA_integer_) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(description = .data$description))) %>%
    arrange(.data$description)
  staging %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("model_type_", hash)),
      row.names = FALSE
    )
  sql.model_type <- paste0("model_type_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  # write new model types
  sprintf("
    INSERT INTO public.model_type
      (fingerprint, description, long_description)
    SELECT
      s.fingerprint,
      s.description,
      s.long_description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.model_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    sql.model_type
  ) %>%
    dbGetQuery(conn = conn)
  # update id in staging table
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      staging.%s AS s
    INNER JOIN
      public.model_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    sql.model_type,
    sql.model_type
  ) %>%
    dbGetQuery(conn = conn)
  # update long_description
  sprintf("
    UPDATE
      public.model_type AS t
    SET
      long_description = s.long_description
    FROM
      staging.%s AS s
    INNER JOIN
      public.model_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      s.long_description IS NOT NULL AND
      t.fingerprint = p.fingerprint
    ",
    sql.model_type
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
    dbCommit(conn)
  }

  staging <- staging %>%
    select(-.data$id)
  attr(staging, "SQL") <- sql.model_type
  attr(staging, "hash") <- hash
  return(staging)
}
