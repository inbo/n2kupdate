context("store_datafield")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
ut.datafield <- data.frame(
  hash = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$id,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
  stringsAsFactors = FALSE
)
ut.location <- data.frame(
  hash = paste0(rep(c("", "child "), each = 2), ut),
  description = c(ut, ut),
  parent_hash = c(NA, NA, ut),
  datafield_hash = ut,
  external_code = rep(ut, each = 2),
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("input is suitable", {
  conn <- connect_db()
  expect_error(
    store_location(location = "junk", datafield = ut.datafield, conn = conn),
    "location does not inherit from class data\\.frame"
  )
  expect_error(
    ut.location %>%
      select_(~-hash) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name hash"
  )
  expect_error(
    ut.location %>%
      select_(~-description) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name description"
  )
  expect_error(
    ut.location %>%
      select_(~-parent_hash) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name parent_hash"
  )
  expect_error(
    ut.location %>%
      select_(~-datafield_hash) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name datafield_hash"
  )
  expect_error(
    ut.location %>%
      select_(~-external_code) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name external_code"
  )
  DBI::dbDisconnect(conn)
})

test_that("it store new data correctly", {
  conn <- connect_db()

  expect_is(
    hash <- store_location(
      location = ut.location,
      datafield = ut.datafield,
      conn = conn
    ),
    "character"
  )

  DBI::dbDisconnect(conn)
})
