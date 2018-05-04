#' Store a vector of datasource types
#' @param datasource_type the vector with datasource types.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr data_frame %>% rowwise mutate select_
#' @importFrom digest sha1
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_datasource_type <- function(datasource_type, hash, conn, clean = TRUE){
  assert_that(is(datasource_type, "character"))
  assert_that(noNA(datasource_type))
  if (missing(hash)) {
    hash <- sha1(list(datasource_type, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  if (clean) {
    dbBegin(conn)
  }

  dst <- data_frame(
    description = sort(unique(datasource_type)),
    id = NA_integer_
  ) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(description = .data$description)))
  dst %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datasource_type_", hash)),
      row.names = FALSE
    )
  datasource_type <- paste0("datasource_type_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.datasource_type
      (fingerprint, description)
    SELECT
      s.fingerprint,
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.datasource_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    datasource_type
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
      public.datasource_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    datasource_type,
    datasource_type
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datasource_type_", hash)))
    dbCommit(conn)
  }

  dst <- dst %>%
    select_(~-id)
  attr(dst, "sql") <- datasource_type
  return(dst)
}
