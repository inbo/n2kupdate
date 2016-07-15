context("store_datasource")
ut.datasource <- data.frame(
  description = c("Unit test datasource 1", "Unit test datasource 2"),
  datasource_type = "PostgreSQL",
  connect_method = "Credentials supplied by the user running the report",
  server = "localhost",
  dbname = "n2kresult",
  stringsAsFactors = FALSE
)
ut.datasource2 <- data.frame(
  description = c("Unit test datasource 1", "Unit test datasource 2"),
  datasource_type = "PostgreSQL",
  connect_method = c(
    "SSH key",
    "Credentials stored in database"
  ),
  server = c("localhost", "AWS"),
  dbname = c("n2kresult_dev", "n2kresult"),
  username = c(NA, "unittest"),
  password = c(NA, "unittest"),
  stringsAsFactors = TRUE
)
ut <- sprintf("unit test %i", 1:2)
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

test_that("it stores new data correctly", {
  conn <- connect_db()
  expect_is(
    hash <- store_datasource(datasource = ut.datasource, conn = conn),
    "character"
  )
  c("staging", paste0("datasource_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datasource_value_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.datasource %>%
    select_(description = ~datasource_type) %>%
    distinct_() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datasource_type")
    )
  datasource_parameters <- ut.datasource %>%
    select_(~-description, ~-datasource_type) %>%
    colnames() %>%
    sort()
  expect_identical(
    datasource_parameters,
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
    select_(~description, ~datasource_type) %>%
    arrange_(~datasource_type, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.description,
          dt.description AS datasource_type
        FROM
          public.datasource AS d
        INNER JOIN
          public.datasource_type AS dt
        ON
          d.datasource_type = dt.id
        ORDER BY
          d.description,
          dt.description;
      ")
    )
  ut.datasource %>%
    gather_(
      key_col = "parameter",
      value_col = "value",
      gather_cols = datasource_parameters,
      na.rm = TRUE
    ) %>%
    arrange_(~description, ~datasource_type, ~parameter) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.description,
          dt.description AS datasource_type,
          dp.description AS parameter,
          dv.value
        FROM
          (
            (
              public.datasource AS d
            INNER JOIN
              public.datasource_type AS dt
            ON
              d.datasource_type = dt.id
            )
          INNER JOIN
            public.datasource_value AS dv
          ON
            dv.datasource = d.id
          )
        INNER JOIN
          public.datasource_parameter AS dp
        ON
          dv.parameter = dp.id
        WHERE
          dv.destroy IS NULL
        ORDER BY
          d.description,
          dt.description,
          dp.description;
      ")
    )

  DBI::dbDisconnect(conn)
})

test_that("subfunctions work correctly", {
  conn <- connect_db()

  # datasource_type
  expect_is(
    datasource_type <- store_datasource_type(datasource_type = ut, conn = conn),
    "SQL"
  )
  datasource_type@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
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
  datasource_parameter@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.datasource %>%
    select_(~-description, ~-datasource_type) %>%
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

test_that("it stores updates data correctly", {
  conn <- connect_db()
  expect_is(
    hash <- store_datasource(datasource = ut.datasource2, conn = conn),
    "character"
  )

  ut.datasource2 %>%
    transmute_(description = ~as.character(datasource_type)) %>%
    distinct_() %>%
    dplyr::anti_join(
      dbGetQuery(conn, "SELECT description FROM public.datasource_type"),
      by = "description"
    ) %>%
    nrow() %>%
    expect_identical(0L)
  datasource_parameters <- ut.datasource2 %>%
    select_(~-description, ~-datasource_type) %>%
    colnames() %>%
    sort()
  data_frame(description = datasource_parameters) %>%
    dplyr::anti_join(
      dbGetQuery(
        conn,
        "SELECT
          description
        FROM
          public.datasource_parameter
        ORDER BY
          description"
      ),
      by = "description"
    ) %>%
    nrow() %>%
    expect_identical(0L)

  ut.datasource2 %>%
    transmute_(
      description = ~as.character(description),
      datasource_type = ~as.character(datasource_type)
    ) %>%
    arrange_(~datasource_type, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.description,
          dt.description AS datasource_type
        FROM
          public.datasource AS d
        INNER JOIN
          public.datasource_type AS dt
        ON
          d.datasource_type = dt.id
        ORDER BY
          d.description,
          dt.description;
      ")
    )
  ut.datasource2 %>%
    mutate_each_(funs(as.character), vars = colnames(ut.datasource2)) %>%
    gather_(
      key_col = "parameter",
      value_col = "value",
      gather_cols = datasource_parameters,
      na.rm = TRUE
    ) %>%
    arrange_(~description, ~datasource_type, ~parameter) %>%
    expect_identical(
      dbGetQuery(
        conn, "
        SELECT
          d.description,
          dt.description AS datasource_type,
          dp.description AS parameter,
          dv.value
        FROM
          (
            public.datasource_value AS dv
          INNER JOIN
            public.datasource_parameter AS dp
          ON
            dv.parameter = dp.id
          )
        INNER JOIN
          (
            public.datasource AS d
          INNER JOIN
            public.datasource_type AS dt
          ON
            d.datasource_type = dt.id
          )
        ON
          dv.datasource = d.id
        WHERE
          dv.destroy IS NULL
        ORDER BY
          d.description, dt.description, dp.description"
      )
    )

  DBI::dbDisconnect(conn)
})
