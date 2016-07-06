#' store a datasource in the database
#' @param datasource a data.frame with datasource metadata
#' @param conn a DBIconnection
#' @export
#' @importFrom assertthat assert_that
#' @details datasource must contain the variable
store_datasource <- function(datasource, conn){
  assert_that(inherits(datasource, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  return(TRUE)
}
