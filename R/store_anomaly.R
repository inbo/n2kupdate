#' Store anomaly
#' @param anomaly a data.frame with variables "anomaly_type_local_id", "datafield", "analyis" and "parameter". "parameter" is optional
#' @inheritParams store_datasource_parameter
#' @inheritParams store_anomaly_type
#' @inheritParams store_datafield
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr %>% anti_join rowwise mutate_ select_ arrange_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbCommit dbBegin
store_anomaly <- function(
  anomaly,
  anomaly_type,
  datafield,
  hash,
  conn,
  clean = TRUE
){
  assert_that(inherits(anomaly, "data.frame"))
  assert_that(has_name(anomaly, "anomaly_type_local_id"))
  assert_that(has_name(anomaly, "datafield_local_id"))
  assert_that(has_name(anomaly, "analysis"))
  assert_that(
    noNA(
      select_(anomaly, ~anomaly_type_local_id, ~datafield_local_id, ~analysis)
    )
  )
  if (missing(hash)) {
    hash <- sha1(list(anomaly, anomaly_type, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  anomaly <- as.character(anomaly)
  if (!has_name(anomaly, "parameter")) {
    anomaly <- anomaly %>%
      mutate_(parameter = NA_integer_)
  }

  if (clean) {
    dbBegin(conn)
  }

  datafield <- tryCatch(
    store_datafield(
      datafield = datafield,
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
  nolink <- anomaly %>%
    anti_join(
      datafield,
      by = c("datafield_local_id" = "local_id")
    ) %>%
    nrow()
  if (nolink > 0) {
    if (clean) {
      dbRollback(conn)
    }
    stop("All anomaly$datafield_local_id must be present in datafield$local_id")
  }
  anomaly_type <- tryCatch(
    store_anomaly_type(
      anomaly_type = anomaly_type,
      hash = hash,
      conn = conn,
      clean = FALSE
    ),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  nolink <- anomaly %>%
    anti_join(
      anomaly_type,
      by = c("anomaly_type_local_id" = "local_id")
    ) %>%
    nrow()
  if (nolink > 0) {
    if (clean) {
      dbRollback(conn)
    }
    stop(
"All anomaly$anomaly_type_local_id must be present in anomaly_type$local_id"
    )
  }

  anomaly <- anomaly %>%
      inner_join(
        datafield %>%
          select_(datafield_local_id = ~local_id, datafield = ~fingerprint),
        by = "datafield_local_id"
      ) %>%
      inner_join(
        anomaly_type %>%
          select_(
            anomaly_type_local_id = ~local_id,
            anomaly_type = ~fingerprint
          ),
        by = "anomaly_type_local_id"
      ) %>%
    select_(
      ~anomaly_type, ~datafield, ~analysis, ~parameter
    ) %>%
    rowwise() %>%
    mutate_(
      fingerprint = ~sha1(c(
        anomaly_type = anomaly_type,
        datafield = datafield,
        analysis = analysis,
        parameter = parameter
      ))
    ) %>%
    arrange_(~fingerprint)
  assert_that(anyDuplicated(anomaly$fingerprint) == 0L)

  anomaly %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("anomaly_", hash)),
      row.names = FALSE
    )
  anomaly.sql <- paste0("anomaly_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.anomaly
      (fingerprint, anomaly_type, datafield, analysis, parameter)
    SELECT
      s.fingerprint,
      pat.id AS anomaly_type,
      pdf.id AS datafield,
      pa.id AS analysis,
      s.parameter
    FROM
      (
        (
          (
            staging.%s AS s
          INNER JOIN
            public.anomaly_type AS pat
          ON
            s.anomaly_type = pat.fingerprint
          )
        INNER JOIN
          public.datafield AS pdf
        ON
          s.datafield = pdf.fingerprint
        )
      INNER JOIN
        public.analysis AS pa
      ON
        s.analysis = pa.file_fingerprint
      )
    LEFT JOIN
      public.anomaly AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    anomaly.sql
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("anomaly_", hash)))
    dbRemoveTable(conn, c("staging", paste0("anomaly_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbCommit(conn)
  }

  attr(anomaly, "SQL") <- anomaly.sql
  attr(anomaly, "hash") <- hash
  return(anomaly)
}
