context("store_datafield")
ut <- sprintf("unit test %i", 1:2)
conn <- connect_db()
ut.datafield <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$id,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
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
  conn <- connect_db()
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
  conn <- connect_db()
  expect_is(
    hash <- store_datafield(datafield = ut.datafield, conn = conn),
    "character"
  )
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
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
    arrange_(~datasource, ~table_name, ~primary_key, ~datafield_type) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.datasource,
          d.table_name,
          d.primary_key,
          dt.description AS datafield_type
        FROM
          public.datafield AS d
        INNER JOIN
          public.datafield_type AS dt
        ON
          d.datafield_type = dt.id
        ORDER BY
          d.datasource,
          d.table_name,
          d.primary_key,
          dt.description;
      ")
    )

  DBI::dbDisconnect(conn)
})

test_that("it ignores existing data", {
  conn <- connect_db()
  ut.datafield$table_name <- factor(ut.datafield$table_name)
  expect_is(
    hash <- store_datafield(
      datafield = ut.datafield,
      conn = conn,
      hash = "junk",
      clean = FALSE
    ),
    "character"
  )
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  ut.datafield %>%
    select_(description = ~datafield_type) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datafield_type")
    )

  factors <- sapply(ut.datafield, is.factor)
  if (any(factors)) {
    ut.datafield <- ut.datafield %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }
  ut.datafield %>%
    select_(~-local_id) %>%
    arrange_(~datasource, ~table_name, ~primary_key, ~datafield_type) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.datasource,
          d.table_name,
          d.primary_key,
          dt.description AS datafield_type
        FROM
          public.datafield AS d
        INNER JOIN
          public.datafield_type AS dt
        ON
          d.datafield_type = dt.id
        ORDER BY
          d.datasource,
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
      p.datasource = s.datasource AND
      p.table_name = s.table_name AND
      p.primary_key = s.primary_key AND
      dt.description = s.datafield_type;",
    hash
  ) %>%
    dbGetQuery(conn = conn)
  expect_identical(results$public_id, results$staging_id)

  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

    DBI::dbDisconnect(conn)
})

test_that("subfunction works correctly", {
  conn <- connect_db()

  # datafield_type
  expect_is(
    datafield_type <- store_datafield_type(datafield_type = ut, conn = conn),
    "SQL"
  )
  datafield_type@.Data %>%
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
