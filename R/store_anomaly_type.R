#' Store anomaly types
#' @param anomaly_type a data.frame with variables "local_id", "description" and "long_description". "long_description" is optional
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate select arrange
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbBegin dbCommit
store_anomaly_type <- function(anomaly_type, hash, conn, clean = TRUE){
  anomaly_type <- character_df(anomaly_type)
  assert_that(has_name(anomaly_type, "local_id"))
  assert_that(has_name(anomaly_type, "description"))
  assert_that(noNA(select(anomaly_type, .data$local_id, .data$description)))
  if (missing(hash)) {
    hash <- sha1(list(anomaly_type, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  if (!has_name(anomaly_type, "long_description")) {
    anomaly_type <- anomaly_type %>%
      mutate(long_description = NA_character_)
  }

  anomaly_type <- anomaly_type %>%
    select(.data$local_id, .data$description, .data$long_description) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(description = .data$description))) %>%
    arrange(.data$fingerprint)
  assert_that(anyDuplicated(anomaly_type$fingerprint) == 0L)

  if (clean) {
    dbBegin(conn)
  }
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
    dbCommit(conn)
  }

  attr(anomaly_type, "SQL") <- anomaly_type.sql
  attr(anomaly_type, "hash") <- hash
  return(anomaly_type)
}
