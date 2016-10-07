#' Store anomaly types
#' @param anomaly_type a data.frame with variables "local_id", "description" and "long_description". "long_description" is optional
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate_ select_ arrange_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_anomaly_type <- function(anomaly_type, hash, conn, clean = TRUE){
  assert_that(inherits(anomaly_type, "data.frame"))
  assert_that(has_name(anomaly_type, "local_id"))
  assert_that(has_name(anomaly_type, "description"))
  assert_that(noNA(select_(anomaly_type, ~local_id, ~description)))
  if (missing(hash)) {
    hash <- sha1(list(anomaly_type, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  anomaly_type <- as.character(anomaly_type)

  if (!has_name(anomaly_type, "long_description")) {
    anomaly_type <- anomaly_type %>%
      mutate_(long_description = NA)
  }

  anomaly_type <- anomaly_type %>%
    select_(~local_id, ~description, ~long_description) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(description = description))) %>%
    arrange_(~fingerprint)
  assert_that(anyDuplicated(anomaly_type$fingerprint) == 0L)

  anomaly_type %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("anomaly_type_", hash)),
      row.names = FALSE
    )
  anomaly_type.sql <- paste0("anomaly_type_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.anomaly_type
      (fingerprint, description, long_description)
    SELECT
      s.fingerprint,
      s.description,
      s.long_description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.anomaly_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    anomaly_type.sql
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      public.anomaly_type AS t
    SET
      long_description = s.long_description
    FROM
      staging.%s AS s
    WHERE
      t.fingerprint = s.fingerprint AND
      s.long_description IS NOT NULL",
    anomaly_type.sql
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("anomaly_type_", hash)))
  }

  attr(anomaly_type, "SQL") <- anomaly_type.sql
  attr(anomaly_type, "hash") <- hash
  return(anomaly_type)
}
