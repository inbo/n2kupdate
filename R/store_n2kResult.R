#' store an n2kResult object into the database
#' @inheritParams store_datasource_parameter
#' @param object a \code{\link[n2kanalysis]{n2kResult-class}} object
#' @export
#' @importFrom methods validObject
#' @importFrom digest sha1
#' @importFrom assertthat assert_that is.string is.flag noNA
#' @importFrom DBI dbBegin dbSendQuery dbRemoveTable dbCommit dbClearResult dbQuoteIdentifier dbWriteTable
#' @importFrom dplyr %>% distinct inner_join select
#' @importFrom rlang .data
#' @importFrom tibble rowid_to_column
store_n2kResult <- function(object, conn, hash, clean = TRUE) {
  assert_that(inherits(object, "n2kResult"))
  validObject(object)

  if (missing(hash)) {
    hash <- sha1(
      list(
        object@AnalysisMetadata, object@AnalysisRelation,
        object@AnalysisVersion, object@RPackage, object@AnalysisVersionRPackage,
        object@AnomalyType, object@Anomaly, object@Parameter,
        object@ParameterEstimate, object@Contrast, object@ContrastCoefficient,
        object@ContrastEstimate, as.POSIXct(Sys.time())
      )
    )
  } else {
    assert_that(is.string(hash))
  }

  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  if (clean) {
    dbBegin(conn)
  }

  object@AnalysisMetadata %>%
    distinct(
      description = .data$ModelType,
      first_year = .data$FirstImportedYear,
      last_year = .data$LastImportedYear,
      duration = .data$Duration
    ) %>%
    rowid_to_column("local_id") -> model_set
  object@AnalysisMetadata %>%
    inner_join(
      model_set,
      by = c(
        "ModelType" = "description",
        "FirstImportedYear" = "first_year",
        "LastImportedYear" = "last_year",
        "Duration" = "duration"
      )
    ) %>%
    select(
      file_fingerprint = .data$FileFingerprint,
      model_set_local_id = .data$local_id,
      location_group = .data$LocationGroupID,
      species_group = .data$SpeciesGroupID,
      last_year = .data$LastAnalysedYear,
      seed = .data$Seed,
      analysis_version = .data$AnalysisVersion,
      analysis_date = .data$AnalysisDate,
      status = .data$Status,
      status_fingerprint = .data$StatusFingerprint
    ) -> analysis
  object@AnalysisRelation %>%
    select(
      analysis = .data$Analysis,
      source_analysis = .data$ParentAnalysis
    ) -> analysis_relation
  tryCatch(
    store_analysis(
      analysis = analysis,
      model_set = model_set,
      analysis_version = object,
      analysis_relation = analysis_relation,
      conn = conn,
      hash = hash,
      clean = FALSE
    ),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  object@Parameter %>%
    select(
      local_id = .data$Fingerprint,
      parent_parameter_local_id = .data$Parent,
      description = .data$Description
    ) -> parameter
  object@AnomalyType %>%
    select(local_id = .data$Fingerprint, description = .data$Description) ->
    anomaly_type
  tryCatch(
    object@Anomaly %>%
      select(
        anomaly_type_local_id = .data$AnomalyType,
        parameter_local_id = .data$Parameter,
        analysis = .data$Analysis,
        observation = .data$Observation
      ) %>%
      store_anomaly(
        anomaly_type = anomaly_type,
        parameter = parameter,
        hash = hash,
        conn = conn,
        clean = FALSE
      ) -> anomaly,
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  object@ParameterEstimate %>%
    rename(
      analysis = .data$Analysis,
      parameter = .data$Parameter,
      estimate = .data$Estimate,
      lcl = .data$LowerConfidenceLimit,
      ucl = .data$UpperConfidenceLimit
    ) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("parameter_estimate_", hash)),
      row.names = FALSE
    )
  parameter_estimate.sql <- paste0("parameter_estimate_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  tryCatch(
    sprintf("
      INSERT INTO public.parameter_estimate
        (parameter, analysis, estimate, lcl, ucl)
      SELECT p.id, a.id, s.estimate, s.lcl, s.ucl
      FROM staging.%s AS s
      INNER JOIN public.parameter AS p ON s.parameter = p.fingerprint
      INNER JOIN public.analysis AS a ON s.analysis = a.file_fingerprint
      LEFT JOIN
        public.parameter_estimate AS pe
      ON
        a.id = pe.analysis AND p.id = pe.parameter
      WHERE pe.parameter IS NULL
      ",
      parameter_estimate.sql
    ) %>%
      dbSendQuery(conn = conn) %>%
      dbClearResult(),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  if (nrow(object@Contrast) > 0) {
    object@Contrast %>%
      left_join(
        object@ContrastEstimate,
        by = c("Fingerprint" = "Contrast")
      ) %>%
      rename(
        local_id = "Fingerprint",
        analysis = "Analysis",
        description = "Description",
        estimate = "Estimate",
        lcl = "LowerConfidenceLimit",
        ucl = "UpperConfidenceLimit"
      ) %>%
      mutate(
        fingerprint = map2_chr(
          .data$analysis,
          .data$description,
          ~sha1(c(analysis = .x, description = .y)),
        )
      ) %>%
      as.data.frame() %>%
      dbWriteTable(
        conn = conn,
        name = c("staging", paste0("contrast_", hash)),
        row.names = FALSE
      )
    contrast.sql <- paste0("contrast_", hash) %>%
      dbQuoteIdentifier(conn = conn)
    object@ContrastCoefficient %>%
      rename(
        contrast_local_id = "Contrast",
        parameter_local_id = "Parameter",
        constant = "Coefficient"
      ) %>%
      as.data.frame() %>%
      dbWriteTable(
        conn = conn,
        name = c("staging", paste0("contrast_coefficient_", hash)),
        row.names = FALSE
      )
    contrast_coefficient.sql <- paste0("contrast_coefficient_", hash) %>%
      dbQuoteIdentifier(conn = conn)
    parameter.sql <- paste0("parameter_", hash) %>%
      dbQuoteIdentifier(conn = conn)
    sprintf("
      INSERT INTO public.contrast
        (fingerprint, analysis, description, estimate, lcl, ucl)
      SELECT
        sc.fingerprint, pa.id AS analysis, sc.description,
        sc.estimate, sc.lcl, sc.ucl
      FROM staging.%s AS sc
      INNER JOIN public.analysis AS pa ON sc.analysis = pa.file_fingerprint
      LEFT JOIN public.contrast AS pc
        ON sc.fingerprint = pc.fingerprint AND pa.id = pc.analysis
      WHERE pc.id IS NULL",
      contrast.sql
    ) %>%
      dbGetQuery(conn = conn)
    sprintf("
      SELECT *
      FROM staging.%s AS scc
      INNER JOIN staging.%s AS sc ON scc.contrast_local_id = sc.local_id
      INNER JOIN public.parameter AS pp ON scc.parameter_local_id = pp.fingerprint",
      contrast_coefficient.sql,
      contrast.sql
    ) %>%
      dbGetQuery(conn = conn)
    sprintf("SELECT * FROM staging.%s", parameter.sql) %>%
      dbGetQuery(conn = conn) %>%
      head()
  }

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("analysis_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_relation_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_version_", hash)))
    dbRemoveTable(conn, c("staging", paste0("avrp_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_set_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("r_package_", hash)))
    dbRemoveTable(conn, c("staging", paste0("status_", hash)))
    dbRemoveTable(conn, c("staging", paste0("parameter_", hash)))
    dbRemoveTable(conn, c("staging", paste0("parameter_estimate_", hash)))
    dbRemoveTable(conn, c("staging", paste0("anomaly_", hash)))
    dbRemoveTable(conn, c("staging", paste0("anomaly_type_", hash)))
    dbCommit(conn)
  }
  return(hash)
}
