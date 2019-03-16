context("store_species")
conn <- connect_ut_db()
ut <- sprintf("unit test %i", 1:2)
ut2 <- sprintf("unit test %i", 3:4)
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
ut.language3 <- data.frame(
  code = c("nl", "en"),
  description = c("Nederlands", "English"),
  stringsAsFactors = FALSE
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
ut.species3 <- data.frame(
  local_id = ut,
  scientific_name = ut,
  nbn_key = ut,
  junk = ut,
  stringsAsFactors = FALSE
)
ut.species4 <- data.frame(
  local_id = ut,
  scientific_name = ut2,
  nbn_key = ut,
  nl = ut2,
  en = ut,
  stringsAsFactors = FALSE
)
ut.source_species_species_dup <- data.frame(
  species_local_id = rep(ut, 2),
  source_species_local_id = rep(ut, 2),
  stringsAsFactors = FALSE
)
ut.source_species_species <- data.frame(
  species_local_id = ut,
  source_species_local_id = ut,
  stringsAsFactors = FALSE
)
ut.source_species_species2 <- data.frame(
  species_local_id = rev(ut),
  source_species_local_id = ut,
  stringsAsFactors = TRUE
)
ut.species_group_species_dup <- data.frame(
  species_local_id = rep(ut, 2),
  species_group_local_id = rep(ut, 2),
  stringsAsFactors = FALSE
)
ut.species_group_species <- data.frame(
  species_local_id = ut,
  species_group_local_id = ut,
  stringsAsFactors = FALSE
)
ut.species_group_species2 <- data.frame(
  species_local_id = rev(ut),
  species_group_local_id = ut,
  stringsAsFactors = TRUE
)
DBI::dbDisconnect(conn)

test_that("store_species_group works correctly", {
  conn <- connect_ut_db()

  expect_is(
    sg <- store_species_group(
      species_group = ut.species_group,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(sg, "hash"),
    "character"
  )
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.species_group %>%
    select(description, scheme) %>%
    arrange(scheme, description) %>%
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
    "data.frame"
  )
  expect_is(
    hash <- attr(sg, "hash"),
    "character"
  )
  expect_identical(
    hash,
    "junk"
  )
  c("staging", "species_group_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "species_group_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  ut.species_group2 %>%
    select(description, scheme) %>%
    character_df() %>%
    arrange(scheme, description) %>%
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
  conn <- connect_ut_db()

  expect_error(
    store_source_species(
      source_species = ut.source_species_dup,
      datafield = ut.datafield,
      hash = "junk",
      conn = conn
    ),
    "Duplicate combinations of datafield_local_id and external_code are found in
source_species."
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  expect_error(
    store_source_species(
      source_species = ut.source_species,
      datafield = "junk",
      hash = "junk",
      conn = conn
    ),
    "datafield does not inherit from class data\\.frame"
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

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
        mutate(description = as.character(description)) %>%
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
        character_df() %>%
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
  conn <- connect_ut_db()

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
    select(code, description) %>%
    arrange(code, description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT code, description
        FROM public.language
        ORDER BY code, description;"
      )
    )

  expect_is(
    lang <- store_language(
      language = ut.language2, conn = conn, clean = FALSE, hash = "junk"
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
    select(code, description) %>%
    character_df() %>%
    arrange(code, description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT code, description
        FROM public.language
        ORDER BY code, description;"
      )
    )

  expect_is(lang <- store_language(language = ut.language3, conn = conn), "SQL")
  dbGetQuery(conn, "SELECT code, description FROM public.language") %>%
    left_join(x = ut.language3, by = "code") -> check
  expect_equal(check$description.x, check$description.y)

  DBI::dbDisconnect(conn)
})

test_that("store_species() works fine", {
  conn <- connect_ut_db()

  expect_error(
    spec <- store_species(
      species = ut.species, language = "junk", hash = "junk", conn = conn
    ),
    "language does not inherit from class data\\.frame"
  )
  hash <- "junk"
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_is(
    spec <- store_species(
      species = ut.species, language = ut.language, conn = conn
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
      s.id, s.scientific_name, s.nbn_key, l.code AS language,
      scn.description AS common
    FROM species AS s
    INNER JOIN species_common_name AS scn ON s.id = scn.species
    INNER JOIN language AS l ON scn.language = l.id
    WHERE scn.destroy IS NULL
  ") %>%
    tidyr::spread(key = "language", value = "common") %>%
    dplyr::full_join(
      ut.species,
      by = c("nbn_key", "scientific_name", "nl", "en")
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.species))

  expect_is(
    spec <- store_species(
      species = ut.species2, language = ut.language, clean = FALSE,
      hash = "junk", conn = conn
    ),
    "data.frame"
  )

  expect_identical(attr(spec, which = "hash", exact = TRUE), "junk")
  c("staging", "language_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "species_common_name_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "species_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  c("staging", "language_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", "species_common_name_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", "species_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  stored <- dbGetQuery(conn, "
    SELECT
      s.id, s.scientific_name, s.nbn_key, l.code AS language,
      scn.description AS common
    FROM species AS s
    INNER JOIN species_common_name AS scn ON s.id = scn.species
    INNER JOIN language AS l ON scn.language = l.id
    WHERE scn.destroy IS NULL
  ") %>%
    tidyr::spread(key = "language", value = "common") %>%
    dplyr::full_join(
      character_df(ut.species2),
      by = c("nbn_key", "scientific_name", "nl", "en")
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.species2))

  expect_error(
    store_species(
      species = ut.species3, language = ut.language, hash = "junk", conn = conn
    ),
    "'junk' is not available is language\\$code"
  )
  hash <- "junk"
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_is(
    spec <- store_species(
      species = ut.species4, language = ut.language, conn = conn
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
      s.id, s.scientific_name, s.nbn_key, l.code AS language,
      scn.description AS common
    FROM species AS s
    INNER JOIN species_common_name AS scn ON s.id = scn.species
    INNER JOIN language AS l ON scn.language = l.id
    WHERE scn.destroy IS NULL
  ") %>%
    tidyr::spread(key = "language", value = "common") %>%
    dplyr::full_join(
      ut.species4,
      by = c("nbn_key", "scientific_name", "nl", "en")
    )
  expect_false(any(is.na(stored$local_id)))
  expect_false(any(is.na(stored$id)))
  expect_identical(nrow(stored), nrow(ut.species4))

  DBI::dbDisconnect(conn)
})

test_that("store_source_species_species() works fine", {
  conn <- connect_ut_db()

  expect_error(
    store_source_species_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species_dup,
      datafield = ut.datafield,
      hash = "junk",
      conn = conn
    ),
    "Duplicate combinations of species_local_id and source_species_local_id are
found in source_species_species."
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  expect_error(
    store_source_species_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = "junk",
      hash = "junk",
      conn = conn
    ),
    "datafield does not inherit from class data\\.frame"
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_is(
    staging.species <- store_source_species_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      conn = conn
    ),
    "data.frame"
  )

  expect_is(
    hash <- attr(staging.species, "hash"),
    "character"
  )

  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(conn, "
    SELECT
      s.id AS species_id,
      s.nbn_key AS nbn_key,
      ss.external_code AS external_code,
      ss.id AS source_species_id
    FROM
      public.source_species AS ss
    INNER JOIN
      (
        public.source_species_species AS sss
      INNER JOIN
        public.species AS s
      ON
        sss.species = s.id
      )
    ON
      sss.source_species = ss.id
    WHERE
      sss.destroy IS NULL
  ") %>%
    dplyr::full_join(
      ut.species %>%
        select(species_local_id = local_id, nbn_key) %>%
        inner_join(
          ut.source_species_species,
          by = "species_local_id"
        ) %>%
        inner_join(
          ut.source_species %>%
            select(source_species_local_id = local_id, external_code),
          by = "source_species_local_id"
        ),
      by = c("nbn_key", "external_code")
    )
  expect_false(any(is.na(stored$species_local_id)))
  expect_false(any(is.na(stored$species_id)))
  expect_false(any(is.na(stored$source_species_local_id)))
  expect_false(any(is.na(stored$source_species_id)))
  expect_identical(nrow(stored), nrow(ut.source_species_species))

  expect_is(
    staging.species <- store_source_species_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species2,
      datafield = ut.datafield,
      clean = FALSE,
      hash = "junk",
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(staging.species, "hash"),
    "character"
  )

  expect_identical(hash, "junk")

  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  c("staging", paste0("language_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  stored <- dbGetQuery(conn, "
    SELECT
      s.id AS species_id,
      s.nbn_key AS nbn_key,
      ss.external_code AS external_code,
      ss.id AS source_species_id
    FROM
      public.source_species AS ss
    INNER JOIN
      (
        public.source_species_species AS sss
      INNER JOIN
        public.species AS s
      ON
        sss.species = s.id
      )
    ON
      sss.source_species = ss.id
    WHERE
      sss.destroy IS NULL
  ") %>%
    dplyr::full_join(
      ut.species %>%
        select(species_local_id = local_id, nbn_key) %>%
        inner_join(
          ut.source_species_species2 %>%
            character_df(),
          by = "species_local_id"
        ) %>%
        inner_join(
          ut.source_species %>%
            select(source_species_local_id = local_id, external_code),
          by = "source_species_local_id"
        ),
      by = c("nbn_key", "external_code")
    )
  expect_false(any(is.na(stored$species_local_id)))
  expect_false(any(is.na(stored$species_id)))
  expect_false(any(is.na(stored$source_species_local_id)))
  expect_false(any(is.na(stored$source_species_id)))
  expect_identical(nrow(stored), nrow(ut.source_species_species2))

  DBI::dbDisconnect(conn)
})

test_that("store_species_group_species() works as expected", {
  conn <- connect_ut_db()

  expect_error(
    store_species_group_species(
      species = "junk",
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = ut.species_group,
      species_group_species = ut.species_group_species,
      hash = "junk",
      conn = conn
    ),
    "species does not inherit from class data\\.frame"
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  expect_error(
    store_species_group_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = "junk",
      species_group_species = ut.species_group_species,
      hash = "junk",
      conn = conn
    ),
    "species_group does not inherit from class data\\.frame"
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_error(
    store_species_group_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = ut.species_group,
      species_group_species = ut.species_group_species_dup,
      hash = "junk",
      conn = conn
    ),
    "Duplicate combinations of species_local_id and species_group_local_id are
found in species_group_species."
  )
  hash <- "junk"
  c("staging", paste0("datafield", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  expect_is(
    output <- store_species_group_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = ut.species_group,
      species_group_species = ut.species_group_species,
      conn = conn
    ),
    "list"
  )
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )

  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_species", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(conn, "
    SELECT
      s.id AS species_id,
      s.nbn_key AS nbn_key,
      sg.id AS species_group_id,
      sg.description AS description,
      sc.fingerprint AS scheme
    FROM
      (
        public.species_group AS sg
      INNER JOIN
        public.scheme AS sc
      ON
        sg.scheme = sc.id
      )
    INNER JOIN
      (
        public.species_group_species AS sgs
      INNER JOIN
        public.species AS s
      ON
        sgs.species = s.id
      )
    ON
      sgs.species_group = sg.id
    WHERE
      sgs.destroy IS NULL
  ") %>%
    dplyr::full_join(
      ut.species %>%
        select(species_local_id = local_id, nbn_key) %>%
        inner_join(
          ut.species_group_species,
          by = "species_local_id"
        ) %>%
        inner_join(
          ut.species_group %>%
            select(species_group_local_id = local_id, description, scheme),
          by = "species_group_local_id"
        ),
      by = c("nbn_key", "description", "scheme")
    )
  expect_false(any(is.na(stored$species_local_id)))
  expect_false(any(is.na(stored$species_id)))
  expect_false(any(is.na(stored$species_group_local_id)))
  expect_false(any(is.na(stored$species_group_id)))
  expect_identical(nrow(stored), nrow(ut.species_group_species))


  expect_is(
    output <- store_species_group_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = ut.species_group,
      species_group_species = ut.species_group_species2,
      conn = conn
    ),
    "list"
  )
  expect_is(
    hash <- attr(output, "hash"),
    "character"
  )

  c("staging", paste0("language_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_common_name_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("species_group_species", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("source_species_species_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("datafield_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(conn, "
    SELECT
      s.id AS species_id,
      s.nbn_key AS nbn_key,
      sg.id AS species_group_id,
      sg.description AS description,
      sc.fingerprint AS scheme
    FROM
      (
        public.species_group AS sg
      INNER JOIN
        public.scheme AS sc
      ON
        sg.scheme = sc.id
      )
    INNER JOIN
      (
        public.species_group_species AS sgs
      INNER JOIN
        public.species AS s
      ON
        sgs.species = s.id
      )
    ON
      sgs.species_group = sg.id
    WHERE
      sgs.destroy IS NULL
  ") %>%
    dplyr::full_join(
      ut.species %>%
        select(species_local_id = local_id, nbn_key) %>%
        inner_join(
          ut.species_group_species2 %>%
            character_df(),
          by = "species_local_id"
        ) %>%
        inner_join(
          ut.species_group %>%
            select(species_group_local_id = local_id, description, scheme),
          by = "species_group_local_id"
        ),
      by = c("nbn_key", "description", "scheme")
    )
  expect_false(any(is.na(stored$species_local_id)))
  expect_false(any(is.na(stored$species_id)))
  expect_false(any(is.na(stored$species_group_local_id)))
  expect_false(any(is.na(stored$species_group_id)))
  expect_identical(nrow(stored), nrow(ut.species_group_species))

  expect_is(
    output <- store_species_group_species(
      species = ut.species,
      language = ut.language,
      source_species = ut.source_species,
      source_species_species = ut.source_species_species,
      datafield = ut.datafield,
      species_group = ut.species_group,
      species_group_species = ut.species_group_species2,
      conn = conn
    ),
    "list"
  )

  DBI::dbDisconnect(conn)
})
