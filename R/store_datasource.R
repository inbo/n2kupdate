#' store a datasource in the database
#' @param datasource a data.frame with datasource metadata
#' @inheritParams store_connect_method
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_
#' @importFrom DBI dbWriteTable dbRemoveTable
#' @details datasource must contain at least the variables description, datasource_type and connect_method.
store_datasource <- function(datasource, conn){
  assert_that(inherits(datasource, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datasource, "description"))
  assert_that(has_name(datasource, "datasource_type"))
  assert_that(has_name(datasource, "connect_method"))

  hash <- sha1(list(datasource, Sys.time()))
  connect_method <- store_connect_method(
    connect_method = datasource$connect_method,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  datasource_type <- store_datasource_type(
    datasource_type = datasource$datasource_type,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  dbRemoveTable(conn, c("staging", paste0("datasource_type_", hash)))
  dbRemoveTable(conn, c("staging", paste0("connect_method_", hash)))
  return(TRUE)
}
