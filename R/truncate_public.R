#' Truncate all tables in the public schema: USE WITH CATION
#' @inheritParams store_connect_method
#' @export
#' @importFrom assertthat assert_that
truncate_public <- function(conn){
  assert_that(inherits(conn, "DBIConnection"))

  c(
    "analysis_dataset", "anomaly", "connect_method", "datafield", "dataset",
    "datasource", "datasource_value", "location", "location_group_location",
    "source_species", "source_species_species"
  ) %>%
    dbQuoteIdentifier(conn = conn) %>%
    sprintf(fmt = "public.%s") %>%
    paste(collapse = ", ") %>%
    sprintf(fmt = "TRUNCATE TABLE %s") %>%
    dbGetQuery(conn = conn)
}
