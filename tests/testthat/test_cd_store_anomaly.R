context("store_anomaly")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
observation <- DBI::dbReadTable(conn, "observation")$fingerprint[1]
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
ut.parameter <- data.frame(
  local_id = ut,
  description = ut,
  parent_parameter_local_id = NA,
  stringsAsFactors = FALSE
)
ut.anomaly <- data.frame(
  anomaly_type_local_id = ut,
  analysis = ut,
  observation = observation,
  parameter_local_id = ut,
  stringsAsFactors = FALSE
)
ut.anomaly2 <- data.frame(
  anomaly_type_local_id = ut,
  analysis = ut,
  observation = c(observation, NA),
  parameter_local_id = c(NA, ut[1]),
  stringsAsFactors = TRUE
)
ut.anomaly_wrong <- data.frame(
  anomaly_type_local_id = "junk",
  analysis = ut,
  observation = observation,
  parameter_local_id = ut,
  stringsAsFactors = FALSE
)
ut.anomaly_wrong2 <- data.frame(
  anomaly_type_local_id = ut,
  observation = "junk",
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
      parameter = ut.parameter,
      conn = conn
    ),
"All anomaly\\$anomaly_type_local_id must be present in anomaly_type\\$local_id"
  )
  expect_error(
    output <- store_anomaly(
      anomaly = ut.anomaly_wrong2,
      anomaly_type = ut.anomaly_type,
      parameter = ut.parameter,
      conn = conn
    ),
    "observations not in database: junk"
  )

  expect_is(
    output <- store_anomaly(
      anomaly = ut.anomaly,
      anomaly_type = ut.anomaly_type,
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
      obs.fingerprint AS observation,
      para.description AS parameter
    FROM
      (
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
      LEFT JOIN
        public.observation AS obs
      ON
        ano.observation = obs.id
      )
    LEFT JOIN
      public.parameter AS para
    ON
      ano.parameter = para.id
    "
  ) %>%
    dplyr::full_join(
      ut.anomaly %>%
        inner_join(
          ut.anomaly_type,
          by = c("anomaly_type_local_id" = "local_id")
        ) %>%
        left_join(
          ut.parameter %>%
            select_(~local_id, parameter = ~description),
          by = c("parameter_local_id" = "local_id")
        ),
      by = c("analysis", "observation", "parameter")
    )
  expect_identical(nrow(stored), nrow(ut.anomaly))
  expect_false(any(is.na(stored$id)))

  expect_is(
    output <- store_anomaly(
      anomaly = ut.anomaly2,
      anomaly_type = ut.anomaly_type,
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
  c("staging", paste0("parameter_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("anomaly_type_", hash)) %>%
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
      obs.fingerprint AS observation,
      para.description AS parameter
    FROM
      (
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
      LEFT JOIN
        public.observation AS obs
      ON
        ano.observation = obs.id
      )
    LEFT JOIN
      public.parameter AS para
    ON
      ano.parameter = para.id
    "
  ) %>%
    dplyr::right_join(
      ut.anomaly2 %>%
        as.character() %>%
        inner_join(
          ut.anomaly_type,
          by = c("anomaly_type_local_id" = "local_id")
        ) %>%
        left_join(
          ut.parameter %>%
            select_(~local_id, parameter = ~description),
          by = c("parameter_local_id" = "local_id")
        ),
      by = c("analysis", "observation", "parameter")
    )
  expect_identical(nrow(stored), nrow(ut.anomaly2))
  expect_false(any(is.na(stored$id)))

  DBI::dbDisconnect(conn)
})
