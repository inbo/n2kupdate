#' Store status levels in the database
#' @param status a character vector with statuses
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate select_ arrange
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable
store_status <- function(status, hash, clean = TRUE, conn){
  if (is.factor(status)) {
    status <- levels(status)[status]
  } else {
    assert_that(is.character(status))
  }
  assert_that(noNA(status))
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))
  if (missing(hash)) {
    hash <- sha1(list(status, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }

  status <- unique(status)

  if (clean) {
    dbBegin(conn)
  }

  staging <- data.frame(
    id = NA_integer_,
    description = status,
    stringsAsFactors = FALSE
  ) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(description = .data$description))) %>%
    arrange(.data$description)
  staging %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("status_", hash)),
      row.names = FALSE
    )
  sql.status <- paste0("status_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.status
      (fingerprint, description)
    SELECT
      s.fingerprint,
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.status AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    sql.status
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      staging.%s AS s
    INNER JOIN
      public.status AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    sql.status,
    sql.status
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("status_", hash)))
    dbCommit(conn)
  }

  staging <- staging %>%
    select_(~-id)
  attr(staging, "SQL") <- sql.status
  attr(staging, "hash") <- hash
  return(staging)
}
