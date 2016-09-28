#' store analysis and dataset in the database
#' @param analysis_dataset A \code{data.frame} linking the \code{file_fingerprint} from \code{analysis} to the \code{fingerprint} from \code{dataset}.
#' @inheritParams store_datasource_parameter
#' @inheritParams store_analysis_version
#' @inheritParams store_model_set
#' @inheritParams store_analysis
#' @inheritParams store_dataset
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom dplyr %>% mutate_each_ funs distinct_ arrange_
#' @importFrom digest sha1
store_analysis_dataset <- function(
  analysis,
  model_set,
  analysis_version,
  dataset,
  analysis_dataset,
  conn
) {
  assert_that(inherits(analysis_dataset, "data.frame"))
  assert_that(has_name(analysis_dataset, "analysis"))
  assert_that(has_name(analysis_dataset, "dataset"))

  factors <- sapply(analysis_dataset, is.factor)
  if (any(factors)) {
    analysis_dataset <- analysis_dataset %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  hash <- sha1(
    list(
      analysis, model_set, analysis_version, dataset, analysis_dataset,
      as.POSIXct(Sys.time())
    )
  )

  store_analysis(
    analysis = analysis,
    model_set = model_set,
    analysis_version = analysis_version,
    hash = hash,
    clean = TRUE,
    conn = conn
  )
  store_dataset(
    dataset = dataset,
    conn = conn
  )

  assert_that(all(analysis_dataset$analysis %in% analysis$file_fingerprint))
  assert_that(all(analysis_dataset$dataset %in% dataset$fingerprint))

  analysis_dataset %>%
    distinct_(~analysis, ~dataset) %>%
    arrange_(~analysis, ~dataset) %>%
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

  stopifnot(
    dbRemoveTable(conn, c("staging", paste0("analysis_dataset_", hash)))
  )

  return(hash)
}
