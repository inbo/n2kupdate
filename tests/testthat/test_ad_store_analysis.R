context("store_analysis")
ut <- sprintf("unit test %i", 1:2)
ut.status <- ut

test_that("store_status works", {
  conn <- connect_db()

  expect_is(
    stored <- store_status(status = c(ut.status, ut.status), conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- DBI::dbReadTable(conn, c("public", "status"))
  expect_equivalent(
    public %>%
      select_(~fingerprint, ~description) %>%
      arrange_(~fingerprint),
    stored %>%
      as.data.frame() %>%
      select_(~fingerprint, ~description) %>%
      arrange_(~fingerprint)
  )
  expect_true(all(ut.status %in% public$description))

  expect_is(
    stored <- store_status(
      status = factor(ut.status),
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "status"))
  expect_equivalent(
    public %>%
      select_(~fingerprint, ~description) %>%
      arrange_(~fingerprint),
    stored %>%
      as.data.frame() %>%
      select_(~fingerprint, ~description) %>%
      arrange_(~fingerprint)
  )
  expect_true(all(ut.status %in% public$description))

  DBI::dbDisconnect(conn)
})
