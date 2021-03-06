#' Store model sets in the database
#' @param model_set a data.frame with the model sets. Must have variables "local_id", "description", "first_year", "last_year" and "duration". The variable "long_description" is optional.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate select inner_join mutate_at funs
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable
store_model_set <- function(model_set, hash, clean = TRUE, conn){
  model_set <- character_df(model_set)
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
      select(
        .data$local_id,
        .data$description,
        .data$first_year,
        .data$last_year,
        .data$duration
      )
  ))

  numbers <- sapply(model_set, is.numeric)
  if (any(numbers)) {
    model_set <- model_set %>%
      mutate_at(.vars = names(numbers)[numbers], .funs = funs(as.integer))
  }
  if (clean) {
    dbBegin(conn)
  }

  if (has_name(model_set, "long_description")) {
    model_type <- tryCatch(
      store_model_type(
        model_set %>%
          select(.data$description, .data$long_description),
        hash = hash,
        clean = FALSE,
        conn = conn
      ),
      error = function(e){
        if (clean) {
          dbRollback(conn)
        }
        stop(e)
      }
    )
  } else {
    model_type <- tryCatch(
      store_model_type(
        model_set %>%
          select(.data$description),
        hash = hash,
        clean = FALSE,
        conn = conn
      ),
      error = function(e){
        if (clean) {
          dbRollback(conn)
        }
        stop(e)
      }
    )
  }

  staging <- model_type %>%
    select(.data$description, model_type = .data$fingerprint) %>%
    inner_join(
      model_set,
      by = "description"
    ) %>%
    select(
      .data$local_id,
      .data$model_type,
      .data$first_year,
      .data$last_year,
      .data$duration
    ) %>%
    mutate(id = NA_integer_) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(
      model_type = .data$model_type,
      first_year = .data$first_year,
      last_year = .data$last_year,
      duration = .data$duration
    ))) %>%
    arrange(.data$model_type, .data$first_year, .data$last_year)
  staging %>%
    select(-.data$local_id) %>%
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
    dbCommit(conn)
  }

  staging <- staging %>%
    select(-.data$id)
  attr(staging, "SQL") <- sql.model_type
  attr(staging, "hash") <- hash
  return(staging)
}
