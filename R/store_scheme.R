#' Store a vector of schemes
#' @param scheme the vector with scheme descriptions.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr data_frame %>%
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_scheme <- function(scheme, hash, conn, clean = TRUE){
  assert_that(is.character(scheme))
  assert_that(noNA(scheme))
  if (missing(hash)) {
    hash <- sha1(list(scheme, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  data_frame(
    description = sort(unique(scheme)),
    id = NA_integer_
  ) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("scheme_", hash)),
      row.names = FALSE
    )
  scheme <- paste0("scheme_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.scheme
      (description)
    SELECT
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.scheme AS p
    ON
      s.description = p.description
    WHERE
      p.id IS NULL",
    scheme
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
      public.scheme AS p
    ON
      s.description = p.description
    WHERE
      t.description = s.description",
    scheme,
    scheme
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("scheme_", hash)))
  }
  return(scheme)
}
