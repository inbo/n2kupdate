#' Store a dataset is the database
#' @param dataset a data.frame with names fingerprint, filename, datasource and import_date
#' @inheritParams store_datasource_parameter
#' @export
store_dataset <- function(dataset, conn){
  assert_that(inherits(dataset, "data.frame"))
  assert_that(has_name(dataset, "fingerprint"))
  assert_that(has_name(dataset, "filename"))
  assert_that(has_name(dataset, "datasource"))
  assert_that(has_name(dataset, "import_date"))

  factors <- sapply(dataset, is.factor)
  if (any(factors)) {
    dataset <- dataset %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  hash <- sha1(
    list(
      dataset, as.POSIXct(Sys.time())
    )
  )

  dataset %>%
    arrange_(~fingerprint) %>%
    mutate_(import_date = ~as.POSIXct(import_date)) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("dataset_", hash)),
      row.names = FALSE
    )
  dataset.sql <- paste0("dataset_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # append new datasets
  sprintf("
    INSERT INTO public.dataset
      (fingerprint, filename, datasource, import_date)
    SELECT
      sd.fingerprint,
      sd.filename,
      pds.id,
      sd.import_date
    FROM
      (
        staging.%s AS sd
      INNER JOIN
        public.datasource AS pds
      ON
        sd.datasource = pds.fingerprint
      )
    LEFT JOIN
      public.dataset AS pd
    ON
      sd.fingerprint = pd.fingerprint
    WHERE
      pd.id IS NULL",
    dataset.sql
  ) %>%
    dbGetQuery(conn = conn)

  stopifnot(
    dbRemoveTable(conn = conn, c("staging", paste0("dataset_", hash)))
  )
  return(hash)
}
