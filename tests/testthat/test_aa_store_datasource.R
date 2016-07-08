context("store_datasource")
require(dplyr, quietly = TRUE)
require(RPostgreSQL, quietly = TRUE)
ut.datasource <- data.frame(
  description = c("Unit test datasource 1", "Unit test datasource 2"),
  datasource_type = "PostgreSQL",
  connect_method = "Credentials supplied by the user running the report",
  server = "localhost",
  dbname = "n2kresult",
  stringsAsFactors = FALSE
)
test_that("input is suitable", {
  expect_error(
    store_datasource(datasource = "junk"),
    "datasource does not inherit from class data\\.frame"
  )
  expect_error(
    store_datasource(datasource = ut.datasource, "junk"),
    "conn does not inherit from class DBIConnection"
  )
  conn <- connect_db()
  expect_error(
    ut.datasource %>%
      select_(~-description) %>%
      store_datasource(conn),
    "datasource does not have name description"
  )
  expect_error(
    ut.datasource %>%
      select_(~-datasource_type) %>%
      store_datasource(conn),
    "datasource does not have name datasource_type"
  )
  expect_error(
    ut.datasource %>%
      select_(~-connect_method) %>%
      store_datasource(conn),
    "datasource does not have name connect_method"
  )
  DBI::dbDisconnect(conn)
})
test_that("it writes the correct data to the staging tables", {
  conn <- connect_db()
  expect_true(store_datasource(datasource = ut.datasource, conn = conn))
  DBI::dbDisconnect(conn)
})
