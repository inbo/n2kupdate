#' store a datasource in the database
#' @param datasource a data.frame with datasource metadata
#' @param conn a DBIconnection
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_
#' @importFrom RPostgreSQL dbWriteTable
#' @details datasource must contain at least the variables description, datasource_type and connect_method.
store_datasource <- function(datasource, conn){
  assert_that(inherits(datasource, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datasource, "description"))
  assert_that(has_name(datasource, "datasource_type"))
  assert_that(has_name(datasource, "connect_method"))

  hash <- sha1(datasource)

  datasource %>%
    transmute_(
      ~connect_method,
      hash = ~hash
    ) %>%
    distinct_() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", "connect_method"),
      append = TRUE,
      row.names = FALSE
    )
  return(TRUE)
}
