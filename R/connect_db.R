#' connect to the unit test database
#' @param host the hostname of the database. Defaults to "localhost".
#' @param dbname the name of the unit test database. Defaults to "n2kunittest".
#' @param user the name of the unit test user. Defaults to "unittest_analysis".
#' @param password the password for the user. Defaults to "unittest".
#' @param port The port of host. Defaults to 5432.
#' @export
#' @importFrom RPostgreSQL dbConnect PostgreSQL
connect_db <- function(
  host = "localhost",
  dbname = "n2kunittest",
  user = "unittest_analysis",
  password = "unittest",
  port = 5432
){
  dbConnect(
    drv = PostgreSQL(),
    host = host,
    dbname = dbname,
    user = user,
    password = password,
    port = port
  )
}
