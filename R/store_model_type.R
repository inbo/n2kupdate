#' Store model type in the database
#' @param model_type a data.frame with the modeltypes. Must have a variable "description". The variable "long_description" is optional.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate_ select_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable
store_model_type <- function(model_type, hash, clean = TRUE, conn){
  assert_that(inherits(model_type, "data.frame"))
  assert_that(has_name(model_type, "description"))
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))
  if (missing(hash)) {
    hash <- sha1(list(model_type, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }

  factors <- sapply(model_type, is.factor)
  if (any(factors)) {
    model_type <- model_type %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }


  if (has_name(model_type, "long_description")) {
    model_type <- model_type %>%
      select_(~description, ~long_description)
  } else {
    model_type <- model_type %>%
      select_(~description) %>%
      mutate_(long_description = ~NA_character_)
  }

  staging <- model_type %>%
    mutate_(id = ~NA_integer_) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(description = description))) %>%
    arrange_(~description)
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
  }

  staging <- staging %>%
    select_(~-id)
  attr(staging, "SQL") <- sql.model_type
  attr(staging, "hash") <- hash
  return(staging)
}
