#' store analysis and dataset in the database
#' @param analysis_dataset A \code{data.frame} linking the \code{file_fingerprint} from \code{analysis} to the \code{fingerprint} from \code{dataset}.
#' @inheritParams store_datasource_parameter
#' @inheritParams store_analysis_version
#' @inheritParams store_model_set
#' @inheritParams store_analysis
#' @inheritParams store_dataset
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% distinct_ arrange
#' @importFrom rlang .data
#' @importFrom digest sha1
#' @importFrom DBI dbBegin dbCommit dbRollback dbWriteTable dbGetQuery
store_analysis_dataset <- function(
  analysis,
  model_set,
  analysis_version,
  dataset,
  analysis_dataset,
  clean = TRUE,
  hash,
  conn
) {
  analysis_dataset <- character_df(analysis_dataset)
  assert_that(has_name(analysis_dataset, "analysis"))
  assert_that(has_name(analysis_dataset, "dataset"))

  if (missing(hash)) {
    hash <- sha1(
      list(
        analysis, model_set, analysis_version, dataset, analysis_dataset,
        as.POSIXct(Sys.time())
      )
    )
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }

  tryCatch(
    store_analysis(
      analysis = analysis,
      model_set = model_set,
      analysis_version = analysis_version,
      hash = hash,
      clean = FALSE,
      conn = conn
    ),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  tryCatch(
    store_dataset(
      dataset = dataset,
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

  assert_that(all(analysis_dataset$analysis %in% analysis$file_fingerprint))
  assert_that(all(analysis_dataset$dataset %in% dataset$fingerprint))

  analysis_dataset %>%
    distinct_(~analysis, ~dataset) %>%
    arrange(.data$analysis, .data$dataset) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("analysis_dataset_", hash)),
      row.names = FALSE
    )
  analysis_dataset.sql <- paste0("analysis_dataset_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # insert new analysis
  sprintf("
    INSERT INTO public.analysis_dataset
      (analysis, dataset)
    SELECT
      pa.id AS analysis,
      pd.id AS dataset
    FROM
      (
        (
          staging.%s AS sad
        INNER JOIN
          public.analysis AS pa
        ON
          sad.analysis = pa.file_fingerprint
        )
      INNER JOIN
        public.dataset AS pd
      ON
        sad.dataset = pd.fingerprint
      )
    LEFT JOIN
      public.analysis_dataset AS pad
    ON
      pa.id = pad.analysis AND
      pd.id = pad.dataset
    WHERE
      pad.analysis IS NULL AND
      pad.dataset IS NULL",
    analysis_dataset.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("analysis_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_version_", hash)))
    dbRemoveTable(conn, c("staging", paste0("avrp_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_set_", hash)))
    dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("r_package_", hash)))
    dbRemoveTable(conn, c("staging", paste0("status_", hash)))
    dbRemoveTable(conn, c("staging", paste0("dataset_", hash)))
    dbRemoveTable(conn, c("staging", paste0("analysis_dataset_", hash)))
    dbCommit(conn)
  }

  return(hash)
}
