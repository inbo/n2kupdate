context("store_species")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
ut.species_group <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "fingerprint"],
  stringsAsFactors = FALSE
)
ut.species_group2 <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "fingerprint"],
  stringsAsFactors = TRUE
)
ut.datafield <- data.frame(
  local_id = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  table_name = ut,
  primary_key = ut,
  datafield_type = "character",
  stringsAsFactors = FALSE
)
ut.source_species <- data.frame(
  local_id = ut,
  description = ut,
  external_code = ut,
  datafield_local_id = ut,
  stringsAsFactors = FALSE
)
ut.source_species2 <- data.frame(
  local_id = ut,
  description = paste(ut, "update"),
  external_code = ut,
  datafield_local_id = ut
)
ut.source_species_dup <- data.frame(
  local_id = ut,
  description = ut,
  external_code = ut[1],
  datafield_local_id = ut[1]
)
ut.language <- data.frame(
  code = c("nl", "en"),
  description = c("Dutch", "English"),
  stringsAsFactors = FALSE
)
ut.language2 <- data.frame(
  code = c("nl", "en"),
  description = c("Dutch", "English"),
  stringsAsFactors = TRUE
)
ut.species <- data.frame(
  local_id = ut,
  scientific_name = ut,
  nbn_key = ut,
  nl = ut,
  en = ut,
  stringsAsFactors = FALSE
)
ut.species2 <- data.frame(
  local_id = ut,
  scientific_name = ut,
  nbn_key = ut,
  nl = rev(ut),
  en = ut,
  stringsAsFactors = TRUE
)
DBI::dbDisconnect(conn)

test_that("store_species_group works correctly", {
  conn <- connect_db()

  expect_is(
    sg <- store_species_group(
      species_group = ut.species_group,
      conn = conn
    ),
    "SQL"
  )
  sg@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.species_group %>%
    select_(~description, ~scheme) %>%
    arrange_(~scheme, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          sg.description,
          s.fingerprint AS scheme
        FROM
          public.species_group AS sg
        INNER JOIN
          public.scheme AS s
        ON
          sg.scheme = s.id
        ORDER BY
          s.fingerprint, sg.description;"
      )
    )

  expect_is(
    sg <- store_species_group(
      species_group = ut.species_group2,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "SQL"
  )
  c("staging", "species_group_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "species_group_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  ut.species_group2 %>%
    select_(~description, ~scheme) %>%
    mutate_each_(funs(as.character), vars = c("scheme", "description")) %>%
    arrange_(~scheme, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          sg.description,
          s.fingerprint AS scheme
        FROM
          public.species_group AS sg
        INNER JOIN
          public.scheme AS s
        ON
          sg.scheme = s.id
        ORDER BY
          s.fingerprint, sg.description;"
      )
    )

  DBI::dbDisconnect(conn)
})

test_that("store_source_species works correctly", {
  conn <- connect_db()

  expect_error(
    store_source_species(
      source_species = ut.source_species_dup,
      datafield = ut.datafield,
      conn = conn
    ),
    "Duplicate combinations of datafield_local_id and external_code are found in
source_species."
  )

  expect_is(
    ss <- store_source_species(
      source_species = ut.source_species,
      datafield = ut.datafield,
      conn = conn
    ),
    "data.frame"
  )

  hash <- attr(ss, which = "hash", exact = TRUE)
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  stored <- dbGetQuery(conn, "
    SELECT
      s.id,
      s.description,
      s.external_code,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key,
      dt.description AS datafield_type
    FROM
      public.source_species AS s
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
      s.datafield = df.id
  ") %>%
    dplyr::full_join(
      ut.source_species %>%
        mutate_(description = ~as.character(description)) %>%
        inner_join(ut.datafield, by = c("datafield_local_id" = "local_id")),
      by = c(
        "description", "external_code", "datasource", "table_name",
        "primary_key", "datafield_type"
      )
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.source_species))

  expect_is(
    ss <- store_source_species(
      source_species = ut.source_species2,
      datafield = ut.datafield,
      hash = "junk",
      conn = conn,
      clean = FALSE
    ),
    "data.frame"
  )
  hash <- attr(ss, which = "hash", exact = TRUE)
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  expect_true(
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
  )
  expect_true(
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
  )
  expect_true(
    dbRemoveTable(conn, c("staging", paste0("source_species_", hash)))
  )

  stored <- dbGetQuery(conn, "
    SELECT
      s.id,
      s.description,
      s.external_code,
      ds.fingerprint AS datasource,
      df.table_name,
      df.primary_key,
      dt.description AS datafield_type
    FROM
      public.source_species AS s
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
      s.datafield = df.id
  ") %>%
    dplyr::full_join(
      ut.source_species2 %>%
        mutate_each_(funs(as.character), dplyr::everything()) %>%
        inner_join(ut.datafield, by = c("datafield_local_id" = "local_id")),
      by = c(
        "description", "external_code", "datasource", "table_name",
        "primary_key", "datafield_type"
      )
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.source_species))

  DBI::dbDisconnect(conn)
})

test_that("store_language() works fine", {
  conn <- connect_db()

  expect_is(
    lang <- store_language(
      language = ut.language,
      conn = conn
    ),
    "SQL"
  )
  lang@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.language %>%
    select_(~code, ~description) %>%
    arrange_(~code, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          code,
          description
        FROM
          public.language
        ORDER BY
          code, description;"
      )
    )

  expect_is(
    lang <- store_language(
      language = ut.language2,
      conn = conn,
      clean = FALSE,
      hash = "junk"
    ),
    "SQL"
  )
  c("staging", "language_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "language_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  ut.language2 %>%
    select_(~code, ~description) %>%
    mutate_each_(funs(as.character), vars = c("code", "description")) %>%
    arrange_(~code, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          code,
          description
        FROM
          public.language
        ORDER BY
          code, description;"
      )
    )

  DBI::dbDisconnect(conn)
})

test_that("store_species() works fine", {
  conn <- connect_db()

  expect_is(
    spec <- store_species(
      species = ut.species,
      language = ut.language,
      conn = conn
    ),
    "data.frame"
  )

  hash <- attr(spec, which = "hash", exact = TRUE)
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()


  stored <- dbGetQuery(conn, "
    SELECT
      s.id,
      s.scientific_name,
      s.nbn_key,
      l.code AS language,
      scn.description AS common
    FROM
      species AS s
    INNER JOIN
      (
        species_common_name AS scn
      INNER JOIN
        language AS l
      ON
        scn.language = l.id
      )
    ON
      s.id = scn.species
    WHERE
      scn.destroy IS NULL
  ") %>%
    tidyr::spread_(key_col = "language", value_col = "common") %>%
    dplyr::full_join(
      ut.species,
      by = c("nbn_key", "scientific_name", "nl", "en")
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.species))

  DBI::dbDisconnect(conn)
})
