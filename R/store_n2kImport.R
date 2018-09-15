#' store an n2kImport object into the database
#' @inheritParams store_datasource_parameter
#' @param object a \code{\link[n2kanalysis]{n2kImport-class}} object
#' @export
#' @importFrom methods validObject
#' @importFrom digest sha1
#' @importFrom assertthat assert_that is.string is.flag noNA
#' @importFrom DBI dbBegin dbSendQuery dbRemoveTable dbCommit dbClearResult dbQuoteIdentifier dbWriteTable
#' @importFrom dplyr %>% distinct inner_join select
#' @importFrom rlang .data
#' @importFrom tibble rowid_to_column
store_n2kImport <- function(object, conn, hash, clean = TRUE) {
  assert_that(inherits(object, "n2kImport"))
  validObject(object)

  if (missing(hash)) {
    hash <- sha1(
      list(
        object@AnalysisMetadata, object@AnalysisRelation,
        object@AnalysisVersion, object@RPackage, object@AnalysisVersionRPackage,
        object@Dataset, as.POSIXct(Sys.time())
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

  tryCatch(
    store_analysis_dataset(
      analysis = analysis,
      model_set = model_set,
      analysis_version = object,
      dataset = object@Dataset,
      analysis_dataset = data.frame(
        analysis = analysis$file_fingerprint,
        dataset = object@Dataset$fingerprint,
        stringsAsFactors = FALSE
      ),
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

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("analysis_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_relation_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_version_", hash)))
    dbRemoveTable(conn, c("staging", paste0("avrp_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_set_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("r_package_", hash)))
    dbRemoveTable(conn, c("staging", paste0("status_", hash)))
    dbRemoveTable(conn, c("staging", paste0("dataset_", hash)))
    dbCommit(conn)
  }
  return(hash)
}
