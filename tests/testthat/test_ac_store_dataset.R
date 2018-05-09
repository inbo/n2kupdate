context("store_dataset")
conn <- connect_ut_db()
ut <- sprintf("unit test %i", 1:2)
ut.dataset <- data.frame(
  fingerprint = ut,
  filename = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  import_date = as.POSIXct(Sys.time()),
  stringsAsFactors = FALSE
)
ut.dataset2 <- data.frame(
  fingerprint = ut,
  filename = rev(ut),
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  import_date = as.POSIXct(Sys.time()),
  stringsAsFactors = TRUE
)
DBI::dbDisconnect(conn)

test_that("stores new datasets", {
  conn <- connect_ut_db()

  expect_is(
    hash <- store_dataset(dataset = ut.dataset, conn = conn),
    "character"
  )

  c("staging", paste0("dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(
      conn = conn, "
      SELECT
        d.fingerprint,
        d.filename,
        ds.fingerprint AS datasource,
        d.import_date
      FROM
        public.dataset AS d
      INNER JOIN
        public.datasource AS ds
      ON
        d.datasource = ds.id"
    )
  expect_identical(
    stored %>%
      select(-import_date),
    ut.dataset %>%
      select(-import_date)
  )

  expect_true(all(stored$import_date - ut.dataset$import_date < 0.1))

  DBI::dbDisconnect(conn)
})

test_that("keeps metadata of existing fingerprints", {
  conn <- connect_ut_db()

  expect_is(
    hash <- store_dataset(dataset = ut.dataset2, conn = conn),
    "character"
  )

  c("staging", paste0("dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(
      conn = conn, "
      SELECT
        d.fingerprint,
        d.filename,
        ds.fingerprint AS datasource,
        d.import_date
      FROM
        public.dataset AS d
      INNER JOIN
        public.datasource AS ds
      ON
        d.datasource = ds.id"
    )
  expect_identical(
    stored %>%
      select(-import_date),
    ut.dataset %>%
      select(-import_date)
  )

  expect_true(all(stored$import_date - ut.dataset$import_date < 0.1))

  DBI::dbDisconnect(conn)
})
