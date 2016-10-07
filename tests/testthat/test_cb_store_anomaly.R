context("store_anomaly")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
ut.anomaly_type <- data.frame(
  local_id = ut,
  description = ut,
  stringsAsFactors = FALSE
)
ut.anomaly_type2 <- data.frame(
  local_id = ut,
  description = ut,
  long_description = c(ut[1], NA)
)
DBI::dbDisconnect(conn)

test_that("store_anomaly_type() works", {
  conn <- connect_db()

  expect_is(
    output <- store_anomaly_type(
      anomaly_type = ut.anomaly_type,
      conn = conn
    ),
    "data.frame"
  )
  expect_true(has_attr(output, "SQL"))
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )

  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      *
    FROM
      public.anomaly_type"
  ) %>%
    dplyr::full_join(
      ut.anomaly_type,
      by = "description"
    )

  expect_identical(nrow(stored), nrow(ut.anomaly_type))
  expect_false(any(is.na(stored$id)))

  expect_is(
    output <- store_anomaly_type(
      anomaly_type = ut.anomaly_type2,
      clean = FALSE,
      hash = "junk",
      conn = conn
    ),
    "data.frame"
  )
  expect_true(has_attr(output, "SQL"))
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )
  expect_identical(hash, "junk")

  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      *
    FROM
      public.anomaly_type"
  ) %>%
    dplyr::full_join(
      ut.anomaly_type2 %>%
        as.character(),
      by = "description"
    )

  expect_identical(nrow(stored), nrow(ut.anomaly_type2))
  expect_false(any(is.na(stored$id)))

  DBI::dbDisconnect(conn)
})
