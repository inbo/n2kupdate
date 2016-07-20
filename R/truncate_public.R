#' Truncate all tables in the public schema: USE WITH CATION
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that
truncate_public <- function(conn){
  assert_that(inherits(conn, "DBIConnection"))

  c(
    "analysis", "analysis_dataset", "analysis_relation", "analysis_version",
    "analysis_version_r_package", "anomaly", "anomaly_type",
    "contrast", "contrast_coefficient", "datafield", "datafield_type",
    "dataset", "datasource", "datasource_parameter", "datasource_type",
    "datasource_value", "language", "location", "location_group",
    "location_group_location", "model_set", "model_type", "parameter",
    "parameter_estimate", "r_package", "scheme", "source_species",
    "source_species_species", "species", "species_common_name", "species_group",
    "species_group_species", "status"
  ) %>%
    dbQuoteIdentifier(conn = conn) %>%
    sprintf(fmt = "public.%s") %>%
    paste(collapse = ", ") %>%
    sprintf(fmt = "TRUNCATE TABLE %s") %>%
    dbGetQuery(conn = conn)
}
