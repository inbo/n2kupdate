context("store_datasource")
require(dplyr, quietly = TRUE)
ut.datasource <- data.frame(
  description = c("Unit test datasource 1", "Unit test datasource 2"),
  datasource_type = "PostgreSQL",
  connect_method = "Credentials supplied by the user running the report",
  server = "localhost",
  dbname = "n2kresult",
  stringsAsFactors = FALSE
)
ut <- sprintf("Unit test %i", 1:2)
test_that("input is suitable", {
  expect_error(
    store_datasource(datasource = "junk"),
    "datasource does not inherit from class data\\.frame"
  )
  expect_error(
    store_datasource(datasource = ut.datasource, "junk"),
    "conn does not inherit from class DBIConnection"
  )
  conn <- connect_db()
  expect_error(
    ut.datasource %>%
      select_(~-description) %>%
      store_datasource(conn),
    "datasource does not have name description"
  )
  expect_error(
    ut.datasource %>%
      select_(~-datasource_type) %>%
      store_datasource(conn),
    "datasource does not have name datasource_type"
  )
  expect_error(
    ut.datasource %>%
      select_(~-connect_method) %>%
      store_datasource(conn),
    "datasource does not have name connect_method"
  )
  DBI::dbDisconnect(conn)
})
test_that("it stores the correct data", {
  conn <- connect_db()
  expect_is(
    hash <- store_datasource(datasource = ut.datasource, conn = conn),
    "character"
  )
  c("staging", paste0("connect_method_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.datasource %>%
    select_(description = ~connect_method) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.connect_method")
    )
  ut.datasource %>%
    select_(description = ~datasource_type) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datasource_type")
    )
  ut.datasource %>%
    select_(~-description, ~-datasource_type, ~-connect_method) %>%
    colnames() %>%
    sort() %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          description
        FROM
          public.datasource_parameter
        ORDER BY
          description"
      )$description
    )
  ut.datasource %>%
    select_(~description, ~datasource_type, ~connect_method) %>%
    arrange_(~datasource_type, ~description, ~connect_method) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.description,
          dt.description AS datasource_type,
          cm.description AS connect_method
        FROM
          (
            public.datasource AS d
          INNER JOIN
            public.datasource_type AS dt
          ON
            d.datasource_type = dt.id
          )
        INNER JOIN
          public.connect_method AS cm
        ON
          d.connect_method = cm.id
        ORDER BY
          d.description,
          dt.description,
          cm.description;
      ")
    )
  DBI::dbDisconnect(conn)
})
test_that("subfunction work correctly", {
  conn <- connect_db()

  # connect_method
  expect_is(
    connect_method <- store_connect_method(connect_method = ut, conn = conn),
    "SQL"
  )
  c(ut.datasource$connect_method, ut) %>%
    unique() %>%
    sort() %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT description FROM public.connect_method"
      )$description
    )
  expect_is(
    connect_method <- store_connect_method(connect_method = ut, conn = conn),
    "SQL"
  )
  expect_false(DBI::dbExistsTable(conn, c("staging", connect_method)))

  # datasource_type
  expect_is(
    datasource_type <- store_datasource_type(datasource_type = ut, conn = conn),
    "SQL"
  )
  expect_false(DBI::dbExistsTable(conn, c("staging", datasource_type)))
  c(ut.datasource$datasource_type, ut) %>%
    unique() %>%
    sort() %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT description FROM public.datasource_type"
      )$description
    )

  # datasource_parameter
  expect_is(
    datasource_parameter <- store_datasource_parameter(
      datasource_parameter = ut,
      conn = conn
    ),
    "SQL"
  )
  expect_false(DBI::dbExistsTable(conn, c("staging", datasource_parameter)))
  ut.datasource %>%
    select_(~-description, ~-datasource_type, ~-connect_method) %>%
    colnames() %>%
    c(ut) %>%
    unique() %>%
    sort() %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          description
        FROM
          public.datasource_parameter
        ORDER BY
          description;"
      )$description
    )

  DBI::dbDisconnect(conn)
})
