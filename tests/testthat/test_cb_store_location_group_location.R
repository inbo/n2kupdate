context("store_location_group_location")
conn <- connect_ut_db()
ut <- sprintf("unit test %i", 1:2)
ut.location_group <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "fingerprint"],
  stringsAsFactors = FALSE
)
ut.datafield <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
  stringsAsFactors = FALSE
)
ut.location <- data.frame(
  local_id = paste0(rep(c("", "child "), each = 2), ut),
  description = c(ut, ut),
  parent_local_id = c(NA, NA, ut),
  datafield_local_id = ut,
  external_code = rep(ut, each = 2),
  stringsAsFactors = FALSE
)
ut.location_group_location <- expand.grid(
  location_local_id = ut.location$local_id,
  location_group_local_id = ut.location_group$local_id,
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("it checks the arguments", {
  conn <- connect_ut_db()
  expect_error(
    store_location_group_location(
      location_group_location = "junk",
      location_group = ut.location_group,
      location = ut.location,
      datafield = ut.datafield,
      conn = conn
    ),
    "location_group_location does not inherit from class data.frame"
  )
  expect_error(
    store_location_group_location(
      location_group_location = ut.location_group_location,
      location_group = "junk",
      location = ut.location,
      datafield = ut.datafield,
      conn = conn
    ),
    "location_group does not inherit from class data.frame"
  )
  DBI::dbDisconnect(conn)
})

test_that("it stores the correct information", {
  conn <- connect_ut_db()

  expect_is(
    staging.location_group <- store_location_group_location(
      location_group_location = ut.location_group_location,
      location_group = ut.location_group,
      location = ut.location,
      datafield = ut.datafield,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(staging.location_group, "hash"),
    "character"
  )
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("location_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("lgl_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  to.store <- ut.location_group_location %>%
    inner_join(
      ut.location_group %>%
        rename(location_group = description),
      by = c("location_group_local_id" = "local_id")
    ) %>%
    select(-location_group_local_id) %>%
    inner_join(
      ut.location,
      by = c("location_local_id" = "local_id")
    ) %>%
    inner_join(
      ut.datafield,
      by = c("datafield_local_id" = "local_id")
    ) %>%
    select(-datafield_local_id)
  stored <- dbGetQuery(conn, "
    SELECT
      s.fingerprint AS scheme,
      lg.description AS location_group,
      l.description,
      l.external_code,
      df.table_name,
      df.primary_key,
      dft.description AS datafield_type,
      ds.fingerprint AS datasource
    FROM
      (
        public.location_group_location AS lgl
      INNER JOIN
        (
          public.location_group AS lg
        INNER JOIN
          public.scheme AS s
        ON
          lg.scheme = s.id
        )
      ON
        lgl.location_group = lg.id
      )
    INNER JOIN
      (
        public.location AS l
      INNER JOIN
        (
          (
            public.datafield AS df
          INNER JOIN
            public.datafield_type AS dft
          ON
            df.datafield_type = dft.id
          )
        INNER JOIN
          public.datasource AS ds
        ON
          df.datasource = ds.id
        )
      ON
        l.datafield = df.id
      )
    ON
      lgl.location = l.id
    WHERE
      lgl.destroy IS NULL")
  result <- to.store %>%
    dplyr::full_join(
      stored %>%
        mutate(junk = 1),
      by = colnames(stored)
    )
  expect_false(any(is.na(result$location_local_id)))
  expect_false(any(is.na(result$junk)))
  expect_identical(nrow(to.store), nrow(stored))
  expect_identical(nrow(to.store), nrow(ut.location_group_location))

  ut.location_group_location$location_local_id <- factor(
    ut.location_group_location$location_local_id
  )
  ut.location_group$description <- factor(ut.location_group$description)
  expect_is(
    staging.location_group <- store_location_group_location(
      location_group_location = ut.location_group_location,
      location_group = ut.location_group,
      location = ut.location,
      datafield = ut.datafield,
      hash = "junk",
      conn = conn,
      clean = FALSE
    ),
    "data.frame"
  )
  expect_identical(
    hash <- attr(staging.location_group, "hash"),
    "junk"
  )
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("lgl_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_group_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("lgl_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  ut.location_group_location$location_local_id  <-
    levels(ut.location_group_location$location_local_id)[
      ut.location_group_location$location_local_id
    ]
  ut.location_group$description <-
    levels(ut.location_group$description)[
      ut.location_group$description
    ]
  to.store <- ut.location_group_location %>%
    inner_join(
      ut.location_group %>%
        rename(location_group = description),
      by = c("location_group_local_id" = "local_id")
    ) %>%
    select(-location_group_local_id) %>%
    inner_join(
      ut.location,
      by = c("location_local_id" = "local_id")
    ) %>%
    inner_join(
      ut.datafield,
      by = c("datafield_local_id" = "local_id")
    ) %>%
    select(-datafield_local_id)
  stored <- dbGetQuery(conn, "
    SELECT
      s.fingerprint AS scheme,
      lg.description AS location_group,
      l.description,
      l.external_code,
      df.table_name,
      df.primary_key,
      dft.description AS datafield_type,
      ds.fingerprint AS datasource
    FROM
      (
        public.location_group_location AS lgl
      INNER JOIN
        (
          public.location_group AS lg
        INNER JOIN
          public.scheme AS s
        ON
          lg.scheme = s.id
        )
      ON
        lgl.location_group = lg.id
      )
    INNER JOIN
      (
        public.location AS l
      INNER JOIN
        (
          (
            public.datafield AS df
          INNER JOIN
            public.datafield_type AS dft
          ON
            df.datafield_type = dft.id
          )
        INNER JOIN
          public.datasource AS ds
        ON
          df.datasource = ds.id
        )
      ON
        l.datafield = df.id
      )
    ON
      lgl.location = l.id
    WHERE
      lgl.destroy IS NULL")
  result <- to.store %>%
    dplyr::full_join(
      stored %>%
        mutate(junk = 1),
      by = colnames(stored)
    )
  expect_false(any(is.na(result$location_local_id)))
  expect_false(any(is.na(result$junk)))
  expect_identical(nrow(to.store), nrow(stored))
  expect_identical(nrow(to.store), nrow(ut.location_group_location))

  DBI::dbDisconnect(conn)
})

test_that("subfunction works correctly", {
  conn <- connect_ut_db()

  # location_group
  expect_error(
    ut.location_group %>%
      select(-local_id) %>%
      store_location_group(conn = conn),
    "location_group does not have name local_id"
  )
  expect_error(
    ut.location_group %>%
      select(-description) %>%
      store_location_group(conn = conn),
    "location_group does not have name description"
  )
  expect_error(
    ut.location_group %>%
      select(-scheme) %>%
      store_location_group(conn = conn),
    "location_group does not have name scheme"
  )
  expect_is(
    staging.location_group <- store_location_group(
      location_group = ut.location_group,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(staging.location_group, "hash"),
    "character"
  )
  expect_is(
    sql <- attr(staging.location_group, "SQL"),
    "SQL"
  )
  sql@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.location_group %>%
    select(description, scheme) %>%
    arrange(scheme, description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          lg.description,
          s.fingerprint AS scheme
        FROM
          public.location_group AS lg
        INNER JOIN
          public.scheme AS s
        ON
          lg.scheme = s.id
        ORDER BY
          s.fingerprint, lg.description;"
      )
    )

  DBI::dbDisconnect(conn)
})
