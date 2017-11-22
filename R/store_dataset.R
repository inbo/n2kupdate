#' Store a dataset is the database
#' @param dataset a data.frame with names fingerprint, filename, datasource and import_date
#' @inheritParams store_datasource_parameter
#' @export
store_dataset <- function(dataset, conn, clean = TRUE, hash){
  dataset <- character_df(dataset)
  assert_that(has_name(dataset, "fingerprint"))
  assert_that(has_name(dataset, "filename"))
  assert_that(has_name(dataset, "datasource"))
  assert_that(has_name(dataset, "import_date"))


  if (missing(hash)) {
    hash <- sha1(
      list(
        dataset, as.POSIXct(Sys.time())
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

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("dataset_", hash)))
    dbCommit(conn)
  }

  return(hash)
}
