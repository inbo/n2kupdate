context("check cleanup")
test_that("all staging tables have been removed", {
  conn <- connect_ut_db()
  tables <- DBI::dbListTables(conn)
  expect_identical(
    grep("_([[:xdigit:]]{40}|junk)$", tables),
    integer(0)
  )
  DBI::dbDisconnect(conn)
})
