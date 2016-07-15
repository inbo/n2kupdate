context("store_scheme")
ut <- sprintf("unit test %i", 1:2)
test_that("input is suitable", {
  expect_error(
    store_scheme(scheme = 0),
    "scheme is not a character vector"
  )
  expect_error(
    store_scheme(scheme = ut, conn = "junk"),
    "conn does not inherit from class DBIConnection"
  )
  conn <- connect_db()
  expect_error(
    store_scheme(scheme = ut, conn = conn, clean = 0),
    "clean is not a flag"
  )
  DBI::dbDisconnect(conn)
})

test_that("it stores new data correctly", {
  conn <- connect_db()
  expect_is(
    hash <- store_scheme(scheme = ut, conn = conn),
    "character"
  )
  c("staging", paste0("scheme_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  data.frame(
    description = ut,
    stringsAsFactors = FALSE
  ) %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.scheme")
    )

  expect_is(
    hash <- store_scheme(
      scheme = ut,
      conn = conn,
      hash = "junk",
      clean = FALSE
    ),
    "character"
  )
  c("staging", "scheme_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  expect_true(
    dbRemoveTable(conn, c("staging", "scheme_junk"))
  )

  data.frame(
    description = ut,
    stringsAsFactors = FALSE
  ) %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.scheme")
    )

  DBI::dbDisconnect(conn)
})

