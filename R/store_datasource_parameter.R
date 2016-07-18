#' Store a vector of datasource parameters
#' @param datasource_parameter the vector with datasource parameters.
#' @param hash the hash of the update session
#' @param conn a DBIconnection
#' @param clean remove the staging table after update. Defaults to TRUE
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr data_frame %>% rowwise select_ mutate_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_datasource_parameter <- function(
  datasource_parameter,
  hash,
  conn,
  clean = TRUE
){
  assert_that(is(datasource_parameter, "character"))
  assert_that(noNA(datasource_parameter))
  if (missing(hash)) {
    hash <- sha1(list(datasource_parameter, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  dsp <- data_frame(
    description = sort(unique(datasource_parameter)),
    id = NA_integer_
  ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(description = description)))
  dsp %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datasource_parameter_", hash)),
      row.names = FALSE
    )
  datasource_parameter <- paste0("datasource_parameter_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.datasource_parameter
      (fingerprint, description)
    SELECT
      s.fingerprint,
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.datasource_parameter AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    datasource_parameter
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
      public.datasource_parameter AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    datasource_parameter,
    datasource_parameter
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datasource_parameter_", hash)))
  }

  dsp <- dsp %>%
    select_(~-id)
  attr(dsp, "sql") <- datasource_parameter
  return(dsp)
}
