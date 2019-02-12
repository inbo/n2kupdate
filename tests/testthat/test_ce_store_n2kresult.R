if (
  requireNamespace("n2kanalysis", quietly = TRUE) &&
    requireNamespace("optimx", quietly = TRUE)
) {
  conn <- connect_ut_db()
  dbGetQuery(conn = conn, "SELECT fingerprint FROM scheme") %>%
    dplyr::pull("fingerprint") %>%
    head(1) -> this_scheme_id
  dbGetQuery(conn = conn, "SELECT fingerprint FROM species_group") %>%
    dplyr::pull("fingerprint") %>%
    head(1) -> this_species_group_id
  dbGetQuery(conn = conn, "SELECT fingerprint FROM location_group") %>%
    dplyr::pull("fingerprint") %>%
    head(1) -> this_location_group_id
  dbGetQuery(conn = conn, "SELECT fingerprint FROM datasource") %>%
    dplyr::pull("fingerprint") %>%
    head(1) -> this_datasource_id
  dbGetQuery(conn = conn, "SELECT fingerprint FROM datafield") %>%
    dplyr::pull("fingerprint") %>%
    head(1) -> this_field_id
  datafield <- data.frame(
    local_id = 1,
    datasource = this_datasource_id,
    table_name = "Seatbelts",
    primary_key = "ID",
    datafield_type = "integer",
    stringsAsFactors = FALSE
  )
  dbGetQuery(conn = conn, "
    SELECT
      id AS local_id,
      description,
      parent_location AS parent_local_id,
      external_code
    FROM location
    WHERE parent_location IS NULL"
  ) %>%
    head(1) %>%
    mutate(datafield_local_id = datafield$local_id) -> location
  dbGetQuery(conn = conn, "
    SELECT
      id as local_id, description, parent_parameter AS parent_parameter_local_id
    FROM parameter
    WHERE parent_parameter IS NULL
  ") %>%
    head(1) -> parameter
  data("Seatbelts")
  Seatbelts %>%
    as.data.frame() %>%
    mutate(
      DatasourceID = sha1(this_datasource_id),
      local_id = seq_len(nrow(Seatbelts)),
      year = ceiling(local_id / 12)
    ) -> Seatbelts
  store_observation(
    conn = conn,
    datafield = datafield,
    observation = Seatbelts %>%
      select(.data$local_id, .data$year) %>%
      mutate(
        datafield_local_id = datafield$local_id,
        external_code = .data$local_id,
        location_local_id = location$local_id,
        parameter_local_id = parameter$local_id
      ),
    location = location,
    parameter = parameter
  ) %>%
    rename(ObservationID = fingerprint) %>%
    inner_join(Seatbelts, by = "local_id") -> Seatbelts
  n2kanalysis::n2k_glmer_poisson(
    result.datasource.id = sha1(sample(letters)),
    scheme.id = this_scheme_id,
    species.group.id = this_species_group_id,
    location.group.id = this_location_group_id,
    model.type = "glmer poisson: year",
    formula = "DriversKilled ~ (1|year)",
    first.imported.year = 1969L,
    last.imported.year = 1985L,
    analysis.date = Sys.time(),
    data = Seatbelts
  ) %>%
    n2kanalysis::fit_model() %>%
    n2kanalysis::get_result() -> object
  expect_is(
    hash <- store_n2kResult(object = object, conn = conn, clean = TRUE),
    "character"
  )
  object@Contrast <- data.frame(
    Fingerprint = "1",
    Description = "junk",
    Analysis = object@AnalysisMetadata$FileFingerprint
  )
  object@ContrastCoefficient <- data.frame(
    Contrast = "1",
    Parameter = object@ParameterEstimate[3:4, "Parameter"],
    Coefficient = 1
  )
  object@ContrastEstimate <- data.frame(
    Contrast = "1",
    Estimate = 0,
    LowerConfidenceLimit = -1,
    UpperConfidenceLimit = 1
  )
  expect_true(validObject(object))
}
