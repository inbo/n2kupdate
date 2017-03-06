context("store_observation")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
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
ut.location <- data.frame(
  local_id = ut,
  description = ut,
  datafield_local_id = ut[1],
  external_code = ut,
  parent_local_id = NA_character_,
  stringsAsFactors = FALSE
)
ut.observation <- data.frame(
  local_id = ut,
  datafield_local_id = ut[2],
  external_code = ut,
  location_local_id = ut,
  year = seq_along(ut),
  parameter_local_id = ut,
  stringsAsFactors = FALSE
)
ut.observation2 <- data.frame(
  local_id = ut,
  datafield_local_id = ut[2],
  external_code = ut,
  location_local_id = ut,
  year = seq_along(ut),
  parameter_local_id = NA,
  stringsAsFactors = FALSE
)

DBI::dbDisconnect(conn)

test_that("store_observation() works", {
  conn <- connect_db()

  expect_is(
    output <- store_observation(
      observation = ut.observation,
      datafield = ut.datafield,
      parameter = ut.parameter,
      location = ut.location,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )

  c("staging", paste0("observation_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      o.id,
      o.fingerprint,
      o.external_code,
      o.year,
      l.description AS location,
      df.table_name,
      df.primary_key,
      dft.description AS datafield_type,
      ds.fingerprint AS datasource,
      p.description AS parameter
    FROM
      (
        (
          public.observation AS o
        INNER JOIN
          public.location AS l
        ON
          o.location = l.id
        )
      LEFT JOIN
        (
          (
            public.datafield AS df
          INNER JOIN
            public.datasource AS ds
          ON
            df.datasource = ds.id
          )
        INNER JOIN
          public.datafield_type AS dft
        ON
          df.datafield_type = dft.id
        )
      ON
        o.datafield = df.id
      )
    INNER JOIN
      public.parameter AS p
    ON
      o.parameter = p.id
    "
  ) %>%
    dplyr::full_join(
      ut.observation %>%
        inner_join(
          ut.location %>%
            select_(~local_id, location = ~description),
          by = c("location_local_id" = "local_id")
        ) %>%
        inner_join(
          ut.datafield,
          by = c("datafield_local_id" = "local_id")
        ) %>%
        inner_join(
          ut.parameter %>%
            select_(~local_id, parameter = ~description),
          by = c("parameter_local_id" = "local_id")
        ),
      by = c(
        "external_code", "year", "location", "table_name", "primary_key",
        "datafield_type", "datasource", "parameter"
      )
    )
  expect_identical(nrow(stored), nrow(ut.observation))
  expect_false(any(is.na(stored$id)))

  expect_is(
    output <- store_observation(
      observation = ut.observation2,
      datafield = ut.datafield,
      parameter = ut.parameter,
      location = ut.location,
      conn = conn,
      clean = FALSE,
      hash = "junk"
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )
  expect_identical(hash, "junk")

  c("staging", paste0("observation_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      o.id,
      o.fingerprint,
      o.external_code,
      o.year,
      l.description AS location,
      df.table_name,
      df.primary_key,
      dft.description AS datafield_type,
      ds.fingerprint AS datasource,
      p.description AS parameter
    FROM
      (
        (
          (
            public.observation AS o
          INNER JOIN
            staging.observation_junk AS so
          ON
            o.fingerprint = so.fingerprint
          )
        INNER JOIN
          public.location AS l
        ON
          o.location = l.id
        )
      LEFT JOIN
        (
          (
            public.datafield AS df
          INNER JOIN
            public.datasource AS ds
          ON
            df.datasource = ds.id
          )
        INNER JOIN
          public.datafield_type AS dft
        ON
          df.datafield_type = dft.id
        )
      ON
        o.datafield = df.id
      )
    LEFT JOIN
      public.parameter AS p
    ON
      o.parameter = p.id
    "
  ) %>%
    dplyr::full_join(
      ut.observation2 %>%
        inner_join(
          ut.location %>%
            select_(~local_id, location = ~description),
          by = c("location_local_id" = "local_id")
        ) %>%
        inner_join(
          ut.datafield,
          by = c("datafield_local_id" = "local_id")
        ),
      by = c(
        "external_code", "year", "location", "table_name", "primary_key",
        "datafield_type", "datasource"
      )
    )
  expect_identical(nrow(stored), nrow(ut.observation2))
  expect_false(any(is.na(stored$id)))

  c("staging", paste0("observation_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  DBI::dbDisconnect(conn)
})
