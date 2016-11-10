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
ut.datafield <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
  stringsAsFactors = FALSE
)
ut.parameter <- data.frame(
  local_id = ut,
  description = ut,
  parent_parameter_local_id = NA,
  stringsAsFactors = FALSE
)
ut.anomaly <- data.frame(
  anomaly_type_local_id = ut,
  datafield_local_id = ut,
  analysis = ut,
  parameter_local_id = ut,
  stringsAsFactors = FALSE
)
ut.anomaly2 <- data.frame(
  anomaly_type_local_id = ut,
  datafield_local_id = ut,
  analysis = ut,
  parameter_local_id = ut,
  stringsAsFactors = TRUE
)
ut.anomaly_wrong <- data.frame(
  anomaly_type_local_id = "junk",
  datafield_local_id = ut,
  analysis = ut,
  parameter_local_id = ut,
  stringsAsFactors = FALSE
)
ut.anomaly_wrong2 <- data.frame(
  anomaly_type_local_id = ut,
  datafield_local_id = "junk",
  analysis = ut,
  parameter_local_id = ut,
  stringsAsFactors = FALSE
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

test_that("store_anomaly() works", {
  conn <- connect_db()

  expect_error(
    output <- store_anomaly(
      anomaly = ut.anomaly_wrong,
      anomaly_type = ut.anomaly_type,
      datafield = ut.datafield,
      parameter = ut.parameter,
      conn = conn
    ),
"All anomaly\\$anomaly_type_local_id must be present in anomaly_type\\$local_id"
  )
  expect_error(
    output <- store_anomaly(
      anomaly = ut.anomaly_wrong2,
      anomaly_type = ut.anomaly_type,
      datafield = ut.datafield,
      parameter = ut.parameter,
      conn = conn
    ),
    "All anomaly\\$datafield_local_id must be present in datafield\\$local_id"
  )

  expect_is(
    output <- store_anomaly(
      anomaly = ut.anomaly,
      anomaly_type = ut.anomaly_type,
      datafield = ut.datafield,
      parameter = ut.parameter,
      conn = conn
    ),
    "data.frame"
  )
  expect_true(has_attr(output, "SQL"))
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )

  c("staging", paste0("anomaly_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      ano.id,
      ano.fingerprint,
      ana.file_fingerprint AS analysis,
      anot.description AS anomaly_type,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key
    FROM
      (
        (
          public.anomaly as ano
        INNER JOIN
          public.analysis AS ana
        ON
          ano.analysis = ana.id
        )
      INNER JOIN
        public.anomaly_type AS anot
      ON
        ano.anomaly_type = anot.id
      )
    INNER JOIN
      (
        public.datafield AS df
      INNER JOIN
        public.datasource AS ds
      ON
        df.datasource = ds.id
      )
    ON
      ano.datafield = df.id
    "
  ) %>%
    dplyr::full_join(
      ut.anomaly %>%
        inner_join(
          ut.datafield,
          by = c("datafield_local_id" = "local_id")
        ) %>%
        inner_join(
          ut.anomaly_type,
          by = c("anomaly_type_local_id" = "local_id")
        ),
      by = c("analysis", "datasource", "table_name", "primary_key")
    )
  expect_identical(nrow(stored), nrow(ut.anomaly))
  expect_false(any(is.na(stored$id)))

  expect_is(
    output <- store_anomaly(
      anomaly = ut.anomaly2,
      anomaly_type = ut.anomaly_type,
      datafield = ut.datafield,
      parameter = ut.parameter,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_true(has_attr(output, "SQL"))
  expect_identical(
    hash <- attr(output, "hash"),
    "junk"
  )

  c("staging", paste0("anomaly_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      ano.id,
      ano.fingerprint,
      ana.file_fingerprint AS analysis,
      anot.description AS anomaly_type,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key
    FROM
      (
        (
          public.anomaly as ano
        INNER JOIN
          public.analysis AS ana
        ON
          ano.analysis = ana.id
        )
      INNER JOIN
        public.anomaly_type AS anot
      ON
        ano.anomaly_type = anot.id
      )
    INNER JOIN
      (
        public.datafield AS df
      INNER JOIN
        public.datasource AS ds
      ON
        df.datasource = ds.id
      )
    ON
      ano.datafield = df.id
    "
  ) %>%
    dplyr::full_join(
      ut.anomaly %>%
        inner_join(
          ut.datafield,
          by = c("datafield_local_id" = "local_id")
        ) %>%
        inner_join(
          ut.anomaly_type,
          by = c("anomaly_type_local_id" = "local_id")
        ),
      by = c("analysis", "datasource", "table_name", "primary_key")
    )
  expect_identical(nrow(stored), nrow(ut.anomaly))
  expect_false(any(is.na(stored$id)))

  DBI::dbDisconnect(conn)
})
