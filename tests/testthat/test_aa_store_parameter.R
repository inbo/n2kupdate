context("store_parameter")
ut <- sprintf("unit test %i", 1:2)
ut.parameter <- data.frame(
  description = ut,
  local_id = ut,
  parent_parameter_local_id = NA,
  stringsAsFactors = FALSE
)
ut.parameter2 <- data.frame(
  description = rep(ut, 2),
  local_id = 1:4,
  parent_parameter_local_id = c(NA, NA, 1:2),
  stringsAsFactors = FALSE
)

test_that("store_parameter works", {
  conn <- connect_ut_db()

  expect_is(
    stored <- store_parameter(parameter = ut.parameter, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_is(
    stored <- store_parameter(parameter = ut.parameter2, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  DBI::dbDisconnect(conn)
})
