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
ut.datasource3 <- data.frame(
  description = "Unit test datasource 1",
  datasource_type = "PostgreSQL",
  connect_method = "Credentials stored in database",
  server = "localhost",
  dbname = "n2kresult_dev",
  username = "unittest",
  password = "unittest",
  stringsAsFactors = FALSE
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
  conn <- connect_ut_db()
  expect_error(
    ut.datasource %>%
      select(-description) %>%
      store_datasource(conn),
    "datasource does not have .*name.*description"
  )
  expect_error(
    ut.datasource %>%
      select(-datasource_type) %>%
      store_datasource(conn),
    "datasource does not have .*name.*datasource_type"
  )
  expect_error(
    ut.datasource %>%
      select(-connect_method) %>%
      store_datasource(conn),
    "datasource does not have .*name.*connect_method"
  )
  DBI::dbDisconnect(conn)
})

test_that("it stores new data correctly", {
  conn <- connect_ut_db()
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
    select(description = datasource_type) %>%
    distinct() %>%
    expect_identical(
      dbGetQuery(conn, "SELECT description FROM public.datasource_type")
    )
  datasource_parameters <- ut.datasource %>%
    select(-description, -datasource_type) %>%
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
    select(description, datasource_type) %>%
    arrange(datasource_type, description) %>%
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
    gather(
      key = "parameter",
      value = "value",
      datasource_parameters,
      na.rm = TRUE
    ) %>%
    arrange(description, datasource_type, parameter) %>%
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
  conn <- connect_ut_db()

  # datasource_type
  expect_is(
    datasource_type <- store_datasource_type(datasource_type = ut, conn = conn),
    "data.frame"
  )
  attr(datasource_type, "sql")@.Data %>%
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
    "data.frame"
  )
  attr(datasource_parameter, "sql")@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.datasource %>%
    select(-description, -datasource_type) %>%
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
  conn <- connect_ut_db()
  expect_is(
    hash <- store_datasource(datasource = ut.datasource2, conn = conn),
    "character"
  )

  ut.datasource2 %>%
    transmute(description = as.character(.data$datasource_type)) %>%
    distinct() %>%
    dplyr::anti_join(
      dbGetQuery(conn, "SELECT description FROM public.datasource_type"),
      by = "description"
    ) %>%
    nrow() %>%
    expect_identical(0L)
  datasource_parameters <- ut.datasource2 %>%
    select(-description, -datasource_type) %>%
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
    transmute(
      description = as.character(description),
      datasource_type = as.character(datasource_type)
    ) %>%
    arrange(datasource_type, description) %>%
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
    character_df() %>%
    gather(
      key = "parameter",
      value = "value",
      datasource_parameters,
      na.rm = TRUE
    ) %>%
    arrange(description, datasource_type, parameter) %>%
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

  expect_is(
    hash <- store_datasource(datasource = ut.datasource3, conn = conn),
    "character"
  )

  "
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
  d.description, dt.description, dp.description" %>%
    dbGetQuery(conn = conn) %>%
    dplyr::count_(~description) %>%
    nrow() %>%
    expect_identical(2L)

  DBI::dbDisconnect(conn)
})
