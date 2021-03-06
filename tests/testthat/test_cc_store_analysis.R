context("store_analysis")
if (requireNamespace("n2kanalysis")) {
conn <- connect_ut_db()
ut <- sprintf("unit test %i", 1:2)
ut.status <- ut
ut.model_type <- data.frame(
  description = ut,
  stringsAsFactors = FALSE
)
ut.model_type2 <- data.frame(
  description = ut,
  long_description = ut,
  stringsAsFactors = TRUE
)
ut.model_type3 <- data.frame(
  description = ut,
  long_description = c(ut[2], NA),
  stringsAsFactors = FALSE
)
ut.model_set <- data.frame(
  local_id = ut,
  description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = 11,
  stringsAsFactors = FALSE
)
ut.model_set2 <- data.frame(
  local_id = ut,
  description = ut,
  long_description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = 11,
  stringsAsFactors = TRUE
)
ut.model_set1e <- data.frame(
  local_id = ut,
  description = ut,
  first_year = 20:21,
  last_year = 10:11,
  duration = 2,
  stringsAsFactors = FALSE
)
ut.model_set2e <- data.frame(
  local_id = ut,
  description = ut,
  first_year = 0:1,
  last_year = 10:11,
  duration = c(110, 2),
  stringsAsFactors = FALSE
)
ut.analysis_version <- n2kanalysis::get_analysis_version(sessionInfo())
ut.analysis_version2 <- ut.analysis_version
ut.analysis_version2@AnalysisVersion <- ut.analysis_version2@AnalysisVersion %>%
  mutate(Fingerprint = factor(Fingerprint))
ut.analysis_version2@RPackage <- ut.analysis_version2@RPackage %>%
  mutate(Fingerprint = factor(Fingerprint))
ut.analysis_version2@AnalysisVersionRPackage <-
  ut.analysis_version2@AnalysisVersionRPackage %>%
    dplyr::mutate_all(funs(factor))
ut.analysis <- data.frame(
  file_fingerprint = ut,
  model_set_local_id = ut.model_set$local_id,
  location_group = DBI::dbReadTable(conn, "location_group")$fingerprint,
  species_group = DBI::dbReadTable(conn, "species_group")$fingerprint,
  last_year = ut.model_set$last_year,
  seed = 1,
  analysis_version = ut.analysis_version@AnalysisVersion$Fingerprint,
  analysis_date = as.POSIXct(Sys.time()),
  status = ut.status,
  status_fingerprint = ut,
  stringsAsFactors = FALSE
)
ut.analysis2 <- data.frame(
  file_fingerprint = ut,
  model_set_local_id = ut.model_set$local_id,
  location_group = DBI::dbReadTable(conn, "location_group")$fingerprint,
  species_group = DBI::dbReadTable(conn, "species_group")$fingerprint,
  last_year = 0,
  seed = 2,
  analysis_version = ut.analysis_version@AnalysisVersion$Fingerprint,
  analysis_date = as.POSIXct(Sys.time()),
  status = ut.status,
  status_fingerprint = ut,
  stringsAsFactors = TRUE
)
ut.analysis_dup <- ut.analysis
ut.analysis_dup$file_fingerprint <- "junk"
ut.dataset <- data.frame(
  fingerprint = ut,
  filename = ut,
  datasource = DBI::dbReadTable(conn, "datasource")$fingerprint,
  import_date = as.POSIXct(Sys.time()),
  stringsAsFactors = FALSE
)
ut.analysis_dataset <- expand.grid(
  analysis = ut.analysis$file_fingerprint,
  dataset = ut.dataset$fingerprint,
  stringsAsFactors = FALSE
)
ut.analysis_dataset2 <- expand.grid(
  analysis = ut.analysis$file_fingerprint,
  dataset = ut.dataset$fingerprint,
  stringsAsFactors = TRUE
)
ut.analysis_relation <- data.frame(
  analysis = ut[1],
  source_analysis = ut[2],
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("store_status works", {
  conn <- connect_ut_db()

  expect_is(
    stored <- store_status(status = c(ut.status, ut.status), conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- DBI::dbReadTable(conn, c("public", "status"))
  expect_equivalent(
    public %>%
      select(fingerprint, description) %>%
      arrange(fingerprint),
    stored %>%
      as.data.frame() %>%
      select(fingerprint, description) %>%
      arrange(fingerprint)
  )
  expect_true(all(ut.status %in% public$description))

  expect_is(
    stored <- store_status(
      status = factor(ut.status),
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "status"))
  expect_equivalent(
    public %>%
      select(fingerprint, description) %>%
      arrange(fingerprint),
    stored %>%
      as.data.frame() %>%
      select(fingerprint, description) %>%
      arrange(fingerprint)
  )
  expect_true(all(ut.status %in% public$description))

  DBI::dbDisconnect(conn)
})

test_that("store_model_type works", {
  conn <- connect_ut_db()

  expect_is(
    stored <- store_model_type(model_type = ut.model_type, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  expect_equivalent(
    public %>%
      select(fingerprint, description, long_description) %>%
      arrange(fingerprint),
    stored %>%
      as.data.frame() %>%
      select(fingerprint, description, long_description) %>%
      arrange(fingerprint)
  )

  expect_is(
    stored <- store_model_type(
      model_type = ut.model_type2,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  expect_equivalent(
    public %>%
      select(fingerprint, description, long_description) %>%
      arrange(fingerprint),
    stored %>%
      as.data.frame() %>%
      select(fingerprint, description, long_description) %>%
      arrange(fingerprint)
  )

  expect_is(
    stored <- store_model_type(
      model_type = ut.model_type3,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- DBI::dbReadTable(conn, c("public", "model_type"))
  combined <- stored %>%
    filter(!is.na(long_description)) %>%
    left_join(
      public,
      by = c("fingerprint", "description")
    )
  expect_identical(
    combined$long_description.x,
    combined$long_description.y
  )
  expect_identical(
    nrow(combined),
    sum(!is.na(ut.model_type3$long_description))
  )

  DBI::dbDisconnect(conn)
})

test_that("store_model_set works", {
  conn <- connect_ut_db()

  expect_error(
    store_model_set(model_set = ut.model_set1e, conn = conn),
    "last_year must be greater or equal to first_year"
  )
  expect_error(
    store_model_set(model_set = ut.model_set2e, conn = conn),
    "duration cannot be larger than last_year \\- first_year \\+ 1"
  )

  expect_is(
    stored <- store_model_set(model_set = ut.model_set, conn = conn),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  public <- dbGetQuery(
    conn = conn, "
    SELECT
      pmt.fingerprint AS model_type,
      pmt.description,
      pmt.long_description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      pms.fingerprint,
      pms.id
    FROM
      public.model_set AS pms
    INNER JOIN
      public.model_type AS pmt
    ON
      pms.model_type = pmt.id"
  )
  stored %>%
    dplyr::anti_join(
      public,
      by = c("model_type", "fingerprint", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)
  ut.model_set %>%
    dplyr::anti_join(
      public,
      by = c("description", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)

  expect_is(
    stored <- store_model_set(
      model_set = ut.model_set2,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "data.frame"
  )
  expect_is(
    hash <- attr(stored, "hash"),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  public <- dbGetQuery(
    conn = conn, "
    SELECT
      pmt.fingerprint AS model_type,
      pmt.description,
      pmt.long_description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      pms.fingerprint,
      pms.id
    FROM
      public.model_set AS pms
    INNER JOIN
      public.model_type AS pmt
    ON
      pms.model_type = pmt.id"
  )
  stored %>%
    dplyr::anti_join(
      public,
      by = c("model_type", "fingerprint", "first_year", "last_year", "duration")
    ) %>%
    nrow() %>%
    expect_identical(0L)
  ut.model_set2 %>%
    mutate_at(
      .vars = c("description", "long_description"),
      .funs = as.character
    ) %>%
    dplyr::anti_join(
      public,
      by = c(
        "description", "long_description", "first_year", "last_year", "duration"
      )
    ) %>%
    nrow() %>%
    expect_identical(0L)

  DBI::dbDisconnect(conn)
})

test_that("store_analysis_version", {
  conn <- connect_ut_db()

  expect_is(
    hash <- store_analysis_version(
      analysis_version = ut.analysis_version,
      conn = conn
    ),
    "character"
  )
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pav.fingerprint AS analysis_version,
      prp.description,
      prp.version,
      prp.origin
    FROM
    (
      public.analysis_version AS pav
    INNER JOIN
      public.analysis_version_r_package AS pavrp
    ON
      pav.id = pavrp.analysis_version
    )
    INNER JOIN
      public.r_package AS prp
    ON
      prp.id = pavrp.r_package"
  )
  ut.analysis_version@AnalysisVersion %>%
    inner_join(
      ut.analysis_version@AnalysisVersionRPackage,
      by = c("Fingerprint" = "AnalysisVersion")
    ) %>%
    inner_join(
      ut.analysis_version@RPackage,
      by = c("RPackage" = "Fingerprint")
    ) %>%
    select(
      analysis_version = Fingerprint,
      description = Description,
      version = Version,
      origin = Origin
    ) %>%
    arrange(.data$description) %>%
    expect_identical(
      stored %>%
        arrange(.data$description)
    )

  expect_is(
    hash <- store_analysis_version(
      analysis_version = ut.analysis_version2,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "character"
  )
  expect_identical(hash, "junk")
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pav.fingerprint AS analysis_version,
      prp.description,
      prp.version,
      prp.origin
    FROM
    (
      public.analysis_version AS pav
    INNER JOIN
      public.analysis_version_r_package AS pavrp
    ON
      pav.id = pavrp.analysis_version
    )
    INNER JOIN
      public.r_package AS prp
    ON
      prp.id = pavrp.r_package"
  )
  ut.analysis_version@AnalysisVersion %>%
    inner_join(
      ut.analysis_version@AnalysisVersionRPackage,
      by = c("Fingerprint" = "AnalysisVersion")
    ) %>%
    inner_join(
      ut.analysis_version@RPackage,
      by = c("RPackage" = "Fingerprint")
    ) %>%
    select(
      analysis_version = Fingerprint,
      description = Description,
      version = Version,
      origin = Origin
    ) %>%
    arrange(.data$description) %>%
    expect_identical(
      stored %>%
        arrange(.data$description)
    )

  DBI::dbDisconnect(conn)
})

test_that("store_analysis() works", {
  conn <- connect_ut_db()

  expect_error(
    store_analysis(
      analysis = ut.analysis_dup,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      conn = conn
    ),
    "Duplicated file_fingerprint"
  )

  expect_is(
    hash <- store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      conn = conn
    ),
    "character"
  )
  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pa.id,
      pa.file_fingerprint,
      pmt.description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      plg.fingerprint AS location_group,
      psg.fingerprint AS species_group,
      pa.last_year AS this_year,
      pa.seed,
      pav.fingerprint AS analysis_version,
      pa.analysis_date,
      ps.description AS status,
      pa.status_fingerprint
    FROM
      (
        public.model_set AS pms
      INNER JOIN
        public.model_type AS pmt
      ON
        pms.model_type = pmt.id
      )
    INNER JOIN
      (
        (
          (
            (
              public.analysis AS pa
            INNER JOIN
              public.location_group AS plg
            ON
              pa.location_group = plg.id
            )
          INNER JOIN
            public.species_group AS psg
          ON
            pa.species_group = psg.id
          )
        INNER JOIN
          public.analysis_version AS pav
        ON
          pa.analysis_version = pav.id
        )
      INNER JOIN
        public.status AS ps
      ON
        pa.status = ps.id
      )
    ON
     pa.model_set = pms.id"
  )
  expect_equal(
    stored %>%
      select(description, first_year, last_year, duration) %>%
      arrange(description),
    ut.model_set %>%
      select(description, first_year, last_year, duration) %>%
      arrange(description)
  )
  expect_equal(
    ut.analysis %>%
      inner_join(
        ut.model_set,
        by = c("model_set_local_id" = "local_id")
      ) %>%
      transmute(
        file_fingerprint,
        description,
        first_year,
        last_year = last_year.y,
        duration,
        location_group,
        species_group,
        this_year = last_year.x,
        seed,
        analysis_version,
        analysis_date = format(analysis_date, format = "%F %T %z"),
        status,
        status_fingerprint
      ) %>%
      arrange(file_fingerprint),
    stored %>%
      select(-id) %>%
      mutate(
        analysis_date = as.POSIXct(analysis_date) %>%
          format(format = "%F %T %z")
      ) %>%
      arrange(file_fingerprint)
  )

  expect_is(
    hash <- store_analysis(
      analysis = ut.analysis2,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "character"
  )
  expect_identical(hash, "junk")

  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pa.id,
      pa.file_fingerprint,
      pmt.description,
      pms.first_year,
      pms.last_year,
      pms.duration,
      plg.fingerprint AS location_group,
      psg.fingerprint AS species_group,
      pa.last_year AS this_year,
      pa.seed,
      pav.fingerprint AS analysis_version,
      pa.analysis_date,
      ps.description AS status,
      pa.status_fingerprint
    FROM
      (
        public.model_set AS pms
      INNER JOIN
        public.model_type AS pmt
      ON
        pms.model_type = pmt.id
      )
    INNER JOIN
      (
        (
          (
            (
              public.analysis AS pa
            INNER JOIN
              public.location_group AS plg
            ON
              pa.location_group = plg.id
            )
          INNER JOIN
            public.species_group AS psg
          ON
            pa.species_group = psg.id
          )
        INNER JOIN
          public.analysis_version AS pav
        ON
          pa.analysis_version = pav.id
        )
      INNER JOIN
        public.status AS ps
      ON
        pa.status = ps.id
      )
    ON
     pa.model_set = pms.id"
  )
  expect_equal(
    stored %>%
      select(description, first_year, last_year, duration) %>%
      arrange(description),
    ut.model_set %>%
      select(description, first_year, last_year, duration) %>%
      arrange(description)
  )
  expect_equal(
    ut.analysis %>%
      inner_join(
        ut.model_set,
        by = c("model_set_local_id" = "local_id")
      ) %>%
      transmute(
        file_fingerprint,
        description,
        first_year,
        last_year = last_year.y,
        duration,
        location_group,
        species_group,
        this_year = last_year.x,
        seed,
        analysis_version,
        analysis_date = format(analysis_date, format = "%F %T %z"),
        status,
        status_fingerprint
      ) %>%
      arrange(file_fingerprint),
    stored %>%
      select(-id) %>%
      mutate(
        analysis_date = as.POSIXct(analysis_date) %>%
          format(format = "%F %T %z")
      ) %>%
      arrange(file_fingerprint)
  )

  DBI::dbDisconnect(conn)
})

test_that("store_analysis_dataset works", {
  conn <- connect_ut_db()

  expect_is(
    hash <- store_analysis_dataset(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      dataset = ut.dataset,
      analysis_dataset = ut.analysis_dataset,
      conn = conn
    ),
    "character"
  )
  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pa.file_fingerprint AS analysis,
      pd.fingerprint AS dataset
    FROM
      (
        public.analysis_dataset AS pad
      INNER JOIN
        public.analysis AS pa
      ON
        pad.analysis = pa.id
      )
    INNER JOIN
      public.dataset AS pd
    ON
      pad.dataset = pd.id"
  )
  expect_equivalent(
    stored %>%
      arrange(analysis, dataset),
    ut.analysis_dataset %>%
      arrange(analysis, dataset)
  )

  expect_is(
    hash <- store_analysis_dataset(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      dataset = ut.dataset,
      analysis_dataset = ut.analysis_dataset2,
      conn = conn
    ),
    "character"
  )
  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_dataset_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()

  stored <- dbGetQuery(
    conn = conn, "
    SELECT
      pa.file_fingerprint AS analysis,
      pd.fingerprint AS dataset
    FROM
      (
        public.analysis_dataset AS pad
      INNER JOIN
        public.analysis AS pa
      ON
        pad.analysis = pa.id
      )
    INNER JOIN
      public.dataset AS pd
    ON
      pad.dataset = pd.id"
  )
  expect_equivalent(
    stored %>%
      arrange(analysis, dataset),
    ut.analysis_dataset %>%
      arrange(analysis, dataset)
  )

  DBI::dbDisconnect(conn)
})

test_that("store_analysis() handles analysis_relations", {
  conn <- connect_ut_db()
  expect_error(
    store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = "junk",
      conn = conn
    )
  )
  expect_error(
    store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = data.frame(junk = "junk"),
      conn = conn
    )
  )
  expect_error(
    store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = data.frame(analysis = "junk"),
      conn = conn
    )
  )
  expect_error(
    store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = data.frame(analysis = ut[1], source_analysis = ut[1]),
      conn = conn
    )
  )
  expect_is(
    hash <- store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = ut.analysis_relation,
      conn = conn
    ),
    "character"
  )
  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_relation_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  expect_identical(
    dbGetQuery(conn = conn, "
      SELECT
        aa.file_fingerprint AS analysis,
        sa.file_fingerprint AS source_analysis
      FROM analysis_relation AS ar
      INNER JOIN analysis AS aa ON ar.analysis = aa.id
      INNER JOIN analysis AS sa ON ar.source_analysis = sa.id
    ") %>%
      anti_join(
        x = ut.analysis_relation,
        by = c("analysis", "source_analysis")
      ) %>%
      nrow(),
    0L
  )

  expect_is(
    hash <- store_analysis(
      analysis = ut.analysis,
      model_set = ut.model_set,
      analysis_version = ut.analysis_version,
      analysis_relation = ut.analysis_relation,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "character"
  )

  expect_identical(hash, "junk")

  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_relation_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()

  c("staging", paste0("analysis_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_relation_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("analysis_version_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("avrp_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_set_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("model_type_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("r_package_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  c("staging", paste0("status_", hash)) %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()

  DBI::dbDisconnect(conn)
})
}
