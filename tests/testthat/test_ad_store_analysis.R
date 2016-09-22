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
ut.model_set <- data.frame(
  description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = 11,
  stringsAsFactors = FALSE
)
ut.model_set2 <- data.frame(
  description = ut,
  long_description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = 11,
  stringsAsFactors = TRUE
)
ut.model_set1e <- data.frame(
  description = ut,
  first_year = 20:21,
  last_year = 10:11,
  duration = 2,
  stringsAsFactors = FALSE
)
ut.model_set2e <- data.frame(
  description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = c(11, 2),
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

test_that("store_model_set works", {
  conn <- connect_db()

  expect_error(
    store_model_set(model_set = ut.model_set1e, conn = conn),
    "last_year must be greater or equal to first_year"
  )
  expect_error(
    store_model_set(model_set = ut.model_set2e, conn = conn),
"duration must be equal to the difference between last_year and first_year \\+ 1" #nolint
  )

  expect_is(
    stored <- store_model_set(model_set = ut.model_set, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- dbGetQuery(
    conn = conn, "
    SELECT
      pmt.fingerprint AS model_type,
      pmt.description,
      pmt.long_description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      pms.fingerprint,
      pms.id
    FROM
      public.model_set AS pms
    INNER JOIN
      public.model_type AS pmt
    ON
      pms.model_type = pmt.id"
  )
  stored %>%
    dplyr::anti_join(
      public,
      by = c("model_type", "fingerprint", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)
  ut.model_set %>%
    dplyr::anti_join(
      public,
      by = c("description", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)

  expect_is(
    stored <- store_model_set(
      model_set = ut.model_set2,
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
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- dbGetQuery(
    conn = conn, "
    SELECT
      pmt.fingerprint AS model_type,
      pmt.description,
      pmt.long_description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      pms.fingerprint,
      pms.id
    FROM
      public.model_set AS pms
    INNER JOIN
      public.model_type AS pmt
    ON
      pms.model_type = pmt.id"
  )
  stored %>%
    dplyr::anti_join(
      public,
      by = c("model_type", "fingerprint", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)
  ut.model_set2 %>%
    dplyr::anti_join(
      public,
      by = c(
        "description", "long_description", "first_year", "last_year", "duration"
      )
    ) %>%
    nrow() %>%
    expect_identical(0L)

  DBI::dbDisconnect(conn)
})
