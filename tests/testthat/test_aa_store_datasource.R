context("store_datasource")
require(dplyr, quietly = TRUE)
require(DBI, quietly = TRUE)
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
  conn <- n2khelper::connect_result(
    username = Sys.getenv("N2KRESULT_USERNAME"),
    password = Sys.getenv("N2KRESULT_PASSWORD")
  )$con
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
  dbDisconnect(conn)
})
test_that("it writes the correct data to the staging tables", {
  conn <- n2khelper::connect_result(
    username = Sys.getenv("N2KRESULT_USERNAME"),
    password = Sys.getenv("N2KRESULT_PASSWORD")
  )$con
  expect_true(store_datasource(datasource = ut.datasource, conn = conn))
  dbDisconnect(conn)
})
