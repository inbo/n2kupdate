context("store_location")
conn <- connect_ut_db()
ut <- sprintf("unit test %i", 1:2)
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
ut.location2 <- data.frame(
  local_id = paste0(rep(c("", "child "), each = 2), ut),
  description = factor(paste0(rep(c("", "update "), each = 2), ut)),
  parent_local_id = c(NA, NA, ut),
  datafield_local_id = ut,
  external_code = rep(ut, each = 2),
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("input is suitable", {
  conn <- connect_ut_db()
  expect_error(
    store_location(location = "junk", datafield = ut.datafield, conn = conn),
    "location does not inherit from class data\\.frame"
  )
  expect_error(
    ut.location %>%
      select(-local_id) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name local_id"
  )
  expect_error(
    ut.location %>%
      select(-description) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name description"
  )
  expect_error(
    ut.location %>%
      select(-parent_local_id) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name parent_local_id"
  )
  expect_error(
    ut.location %>%
      select(-datafield_local_id) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name datafield_local_id"
  )
  expect_error(
    ut.location %>%
      select(-external_code) %>%
      store_location(datafield = ut.datafield, conn = conn),
    "location does not have name external_code"
  )
  ut.duplicate <- rbind(ut.location, ut.location) %>%
    mutate(local_id = letters[seq_along(local_id)])
  expect_error(
    store_location(
      location = ut.duplicate,
      datafield = ut.datafield,
      conn = conn
    ),
    "Duplicate combinations of datafield_local_id, external_code and
parent_local_id are found in location."
  )
  DBI::dbDisconnect(conn)
})

test_that("it stores new data correctly", {
  conn <- connect_ut_db()

  expect_is(
    stored.location <- store_location(
      location = ut.location,
      datafield = ut.datafield,
      conn = conn
    ),
    "data.frame"
  )
  hash <- attr(stored.location, which = "hash", exact = TRUE)
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(conn, "
    SELECT
      l.id,
      l.parent_location,
      l.description,
      l.external_code,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key,
      dt.description AS datafield_type
    FROM
      public.location AS l
    INNER JOIN
      (
        (
          public.datafield AS df
        INNER JOIN
          public.datasource AS ds
        ON
          df.datasource = ds.id
        )
      INNER JOIN
        public.datafield_type AS dt
      ON
        df.datafield_type = dt.id
      )
    ON
      l.datafield = df.id
  ") %>%
    dplyr::full_join(
      ut.location %>%
        inner_join(ut.datafield, by = c("datafield_local_id" = "local_id")),
      by = c(
        "description", "external_code", "datasource", "table_name",
        "primary_key", "datafield_type"
      )
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.location))
  expect_identical(is.na(stored$parent_local_id), is.na(stored$parent_location))
  test <- stored %>%
    left_join(
      stored %>%
        select(test = id, parent_local_id = local_id),
      by = "parent_local_id"
    )
  expect_identical(test$parent_location, test$test)

  DBI::dbDisconnect(conn)
})

test_that("it updates the description of existing locations", {
  conn <- connect_ut_db()

  expect_is(
    stored.location <- store_location(
      location = ut.location2,
      datafield = ut.datafield,
      conn = conn,
      hash = "junk",
      clean = FALSE
    ),
    "data.frame"
  )
  hash <- attr(stored.location, which = "hash", exact = TRUE)
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("location_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  expect_true(
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
  )
  expect_true(
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
  )
  expect_true(
    dbRemoveTable(conn, c("staging", paste0("location_", hash)))
  )

  stored <- dbGetQuery(conn, "
    SELECT
      l.id,
      l.parent_location,
      l.description,
      l.external_code,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key,
      dt.description AS datafield_type
    FROM
      public.location AS l
    INNER JOIN
      (
        (
          public.datafield AS df
        INNER JOIN
          public.datasource AS ds
        ON
          df.datasource = ds.id
        )
      INNER JOIN
        public.datafield_type AS dt
      ON
        df.datafield_type = dt.id
      )
    ON
      l.datafield = df.id
  ") %>%
    dplyr::full_join(
      ut.location2 %>%
        mutate(description = as.character(description)) %>%
        inner_join(ut.datafield, by = c("datafield_local_id" = "local_id")),
      by = c(
        "description", "external_code", "datasource", "table_name",
        "primary_key", "datafield_type"
      )
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.location2))
  expect_identical(is.na(stored$parent_local_id), is.na(stored$parent_location))
  test <- stored %>%
    left_join(
      stored %>%
        select(test = id, parent_local_id = local_id),
      by = "parent_local_id"
    )
  expect_identical(test$parent_location, test$test)

  DBI::dbDisconnect(conn)
})

conn <- connect_ut_db()
c("staging", "datafield_junk") %>%
  DBI::dbExistsTable(conn = conn) %>%
  expect_false()
DBI::dbDisconnect(conn)
