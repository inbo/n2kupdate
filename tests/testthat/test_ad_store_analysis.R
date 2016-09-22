context("store_analysis")
ut <- sprintf("unit test %i", 1:2)
ut.status <- ut
ut.model_type <- data.frame(
  description = ut,
  stringsAsFactors = FALSE
)
ut.model_type2 <- data.frame(
  description = ut,
  long_description = ut,
  stringsAsFactors = TRUE
)
ut.model_type3 <- data.frame(
  description = ut,
  long_description = c(ut[2], NA),
  stringsAsFactors = FALSE
)

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

test_that("store_model_type works", {
  conn <- connect_db()

  expect_is(
    stored <- store_model_type(model_type = ut.model_type, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  expect_equivalent(
    public %>%
      select_(~fingerprint, ~description, ~long_description) %>%
      arrange_(~fingerprint),
    stored %>%
      as.data.frame() %>%
      select_(~fingerprint, ~description, ~long_description) %>%
      arrange_(~fingerprint)
  )

  expect_is(
    stored <- store_model_type(
      model_type = ut.model_type2,
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
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  expect_equivalent(
    public %>%
      select_(~fingerprint, ~description, ~long_description) %>%
      arrange_(~fingerprint),
    stored %>%
      as.data.frame() %>%
      select_(~fingerprint, ~description, ~long_description) %>%
      arrange_(~fingerprint)
  )

  expect_is(
    stored <- store_model_type(
      model_type = ut.model_type3,
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
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  combined <- stored %>%
    filter_(~!is.na(long_description)) %>%
    left_join(
      public,
      by = c("fingerprint", "description")
    )
  expect_identical(
    combined$long_description.x,
    combined$long_description.y
  )
  expect_identical(
    nrow(combined),
    sum(!is.na(ut.model_type3$long_description))
  )

  DBI::dbDisconnect(conn)
})
