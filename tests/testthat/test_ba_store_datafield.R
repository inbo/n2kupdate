context("store_datafield")
ut <- sprintf("unit test %i", 1:2)
conn <- connect_ut_db()
ut.datafield <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
  stringsAsFactors = FALSE
)
ut.datafield_error <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  table_name = ut,
  primary_key = ut,
  datafield_type = 123,
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("input is suitable", {
  expect_error(
    store_datafield(datafield = "junk"),
    "datafield does not inherit from class data\\.frame"
  )
  expect_error(
    store_datafield(datafield = ut.datafield, "junk"),
    "conn does not inherit from class DBIConnection"
  )
  conn <- connect_ut_db()
  expect_error(
    store_datafield(datafield = ut.datafield_error, conn = conn),
    "datafield_type is not a character vector"
  )
  expect_error(
    ut.datafield %>%
      select_(~-local_id) %>%
      store_datafield(conn),
    "datafield does not have name local_id"
  )
  expect_error(
    ut.datafield %>%
      select_(~-datasource) %>%
      store_datafield(conn),
    "datafield does not have name datasource"
  )
  expect_error(
    ut.datafield %>%
      select_(~-datafield_type) %>%
      store_datafield(conn),
    "datafield does not have name datafield_type"
  )
  expect_error(
    ut.datafield %>%
      select_(~-table_name) %>%
      store_datafield(conn),
    "datafield does not have name table_name"
  )
  expect_error(
    ut.datafield %>%
      select_(~-primary_key) %>%
      store_datafield(conn),
    "datafield does not have name primary_key"
  )
  DBI::dbDisconnect(conn)
})

test_that("it stores new data correctly", {
  conn <- connect_ut_db()
  expect_is(
    hash <- store_datafield(datafield = ut.datafield, conn = conn),
    "data.frame"
  )
  paste0("datafield_type_", attr(hash, "hash")) %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  paste0("datafield_", attr(hash, "hash")) %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.datafield %>%
    select_(description = ~datafield_type) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datafield_type")
    )
  ut.datafield %>%
    select_(~-local_id) %>%
    arrange(datasource, table_name, primary_key, datafield_type) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          ds.fingerprint AS datasource,
          d.table_name,
          d.primary_key,
          dt.description AS datafield_type
        FROM
          (
            public.datafield AS d
          INNER JOIN
            public.datasource AS ds
          ON
            d.datasource = ds.id
          )
        INNER JOIN
          public.datafield_type AS dt
        ON
          d.datafield_type = dt.id
        ORDER BY
          ds.fingerprint,
          d.table_name,
          d.primary_key,
          dt.description;
      ")
    )

  DBI::dbDisconnect(conn)
})

conn <- connect_ut_db()
c("staging", "datafield_junk") %>%
  DBI::dbExistsTable(conn = conn) %>%
  expect_false()
DBI::dbDisconnect(conn)

test_that("it ignores existing data", {
  conn <- connect_ut_db()
  ut.datafield$table_name <- factor(ut.datafield$table_name)
  expect_is(
    hash <- store_datafield(
      datafield = ut.datafield,
      conn = conn,
      hash = "junk",
      clean = FALSE
    ),
    "data.frame"
  )

  ut.datafield %>%
    select_(description = ~datafield_type) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datafield_type")
    )

  ut.datafield %>%
    character_df() %>%
    select_(~-local_id) %>%
    arrange(datasource, table_name, primary_key, datafield_type) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          ds.fingerprint AS datasource,
          d.table_name,
          d.primary_key,
          dt.description AS datafield_type
        FROM
          (
            public.datafield AS d
          INNER JOIN
            public.datasource AS ds
          ON
            d.datasource = ds.id
          )
        INNER JOIN
          public.datafield_type AS dt
        ON
          d.datafield_type = dt.id
        ORDER BY
          ds.fingerprint,
          d.table_name,
          d.primary_key,
          dt.description;
      ")
    )

  results <- sprintf("
    SELECT
      p.id AS public_id,
      s.id AS staging_id
    FROM
      (
        public.datafield AS p
      INNER JOIN
        public.datafield_type AS dt
      ON
        p.datafield_type = dt.id
      )
    INNER JOIN
      staging.datafield_%s AS s
    ON
      p.fingerprint = s.fingerprint;",
    attr(hash, "hash")
  ) %>%
    dbGetQuery(conn = conn)
  expect_identical(results$public_id, results$staging_id)

  c("staging", paste0("datafield_type_", attr(hash, "hash"))) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", attr(hash, "hash"))) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", attr(hash, "hash"))) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", attr(hash, "hash"))) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

    DBI::dbDisconnect(conn)
})

conn <- connect_ut_db()
c("staging", "datafield_junk") %>%
  DBI::dbExistsTable(conn = conn) %>%
  expect_false()
DBI::dbDisconnect(conn)

test_that("subfunction works correctly", {
  conn <- connect_ut_db()

  # datafield_type
  expect_is(
    datafield_type <- store_datafield_type(datafield_type = ut, conn = conn),
    "data.frame"
  )
  attr(datafield_type, "SQL")@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c(ut.datafield$datafield_type, ut) %>%
    unique() %>%
    sort() %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT description FROM public.datafield_type"
      )$description
    )

  DBI::dbDisconnect(conn)
})

conn <- connect_ut_db()
c("staging", "datafield_junk") %>%
  DBI::dbExistsTable(conn = conn) %>%
  expect_false()
DBI::dbDisconnect(conn)
