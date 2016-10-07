#' Store model sets in the database
#' @param model_set a data.frame with the model sets. Must have variables "local_id", "description", "first_year", "last_year" and "duration". The variable "long_description" is optional.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate_ select_ arrange_ inner_join mutate_each_ funs
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable
store_model_set <- function(model_set, hash, clean = TRUE, conn){
  assert_that(inherits(model_set, "data.frame"))
  assert_that(has_name(model_set, "local_id"))
  assert_that(has_name(model_set, "description"))
  assert_that(has_name(model_set, "first_year"))
  assert_that(has_name(model_set, "last_year"))
  assert_that(has_name(model_set, "duration"))
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))
  if (missing(hash)) {
    hash <- sha1(list(model_set, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  if (any(model_set$first_year > model_set$last_year)) {
    stop("last_year must be greater or equal to first_year")
  }
  if (
    any(model_set$duration > (model_set$last_year - model_set$first_year + 1))
  ) {
    stop("duration cannot be larger than last_year - first_year + 1")
  }
  assert_that(noNA(
    model_set %>%
      select_(~local_id, ~description, ~first_year, ~last_year, ~duration)
  ))

  model_set <- as.character(model_set)
  numbers <- sapply(model_set, is.numeric)
  if (any(numbers)) {
    model_set <- model_set %>%
      mutate_each_(funs(as.integer), vars = names(numbers)[numbers])
  }

  if (has_name(model_set, "long_description")) {
    model_type <- store_model_type(
      model_set %>%
        select_(~description, ~long_description),
      hash = hash,
      clean = FALSE,
      conn = conn
    )
  } else {
    model_type <- store_model_type(
      model_set %>%
        select_(~description),
      hash = hash,
      clean = FALSE,
      conn = conn
    )
  }

  staging <- model_type %>%
    select_(~description, model_type = ~fingerprint) %>%
    inner_join(
      model_set,
      by = "description"
    ) %>%
    select_(~local_id, ~model_type, ~first_year, ~last_year, ~duration) %>%
    mutate_(id = ~NA_integer_) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(
      model_type = model_type,
      first_year = first_year,
      last_year = last_year,
      duration = duration
    ))) %>%
    arrange_(~model_type, ~first_year, ~last_year)
  staging %>%
    select_(~-local_id) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("model_set_", hash)),
      row.names = FALSE
    )
  sql.model_type <- attr(model_type, "SQL")
  sql.model_set <- paste0("model_set_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  # write new model set
  sprintf("
    INSERT INTO public.model_set
      (fingerprint, model_type, first_year, last_year, duration)
    SELECT
      s.fingerprint,
      t.id AS model_type,
      s.first_year,
      s.last_year,
      s.duration
    FROM
      (
        staging.%s AS s
      INNER JOIN
        staging.%s AS t
      ON
        s.model_type = t.fingerprint
      )
    LEFT JOIN
      public.model_set AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    sql.model_set,
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
      (
        staging.%s AS s
      INNER JOIN
        staging.%s AS st
      ON
        s.model_type = st.fingerprint
      )
    INNER JOIN
      public.model_set AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    sql.model_set,
    sql.model_set,
    sql.model_type
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_set_", hash)))
  }

  staging <- staging %>%
    select_(~-id)
  attr(staging, "SQL") <- sql.model_type
  attr(staging, "hash") <- hash
  return(staging)
}
