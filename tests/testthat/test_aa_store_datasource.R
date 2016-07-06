context("store_datasource")
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
})
