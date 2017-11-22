#' Store anomaly
#' @param anomaly a data.frame with variables "anomaly_type_local_id", "datafield", "analyis" and "parameter_local_id".
#' @inheritParams store_datasource_parameter
#' @inheritParams store_anomaly_type
#' @inheritParams store_datafield
#' @inheritParams store_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr %>% anti_join rowwise mutate_ select_ arrange_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbCommit dbBegin
store_anomaly <- function(
  anomaly,
  anomaly_type,
  parameter,
  hash,
  conn,
  clean = TRUE
){
  anomaly <- character_df(anomaly)
  assert_that(has_name(anomaly, "anomaly_type_local_id"))
  assert_that(has_name(anomaly, "parameter_local_id"))
  assert_that(has_name(anomaly, "analysis"))
  assert_that(has_name(anomaly, "observation"))
  assert_that(
    noNA(
      select_(anomaly, ~anomaly_type_local_id, ~analysis)
    )
  )
  both_na <- anomaly %>%
    filter_(~is.na(parameter_local_id), ~is.na(observation)) %>%
    nrow()
  if (both_na > 0) {
    stop("each row must contain either parameter_local_id or observation")
  }
  if (missing(parameter)) {
    parameter <- NULL
  }

  if (missing(hash)) {
    hash <- sha1(list(
      anomaly, anomaly_type, parameter, as.POSIXct(Sys.time())
    ))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  if (clean) {
    dbBegin(conn)
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
  if (is.null(parameter)) {
    parameter <- data.frame(
      local_id = character(0),
      fingerprint = character(0),
      stringsAsFactors = FALSE
    )
  } else {
    parameter <- tryCatch(
      store_parameter(
        parameter = parameter,
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
      filter_(~!is.na(parameter_local_id)) %>%
      anti_join(
        parameter,
        by = c("parameter_local_id" = "local_id")
      ) %>%
      nrow()
    if (nolink > 0) {
      if (clean) {
        dbRollback(conn)
      }
      stop(
        "All anomaly$parameter_local_id must be present in parameter$local_id"
      )
    }
  }

  anomaly <- anomaly %>%
    inner_join(
      anomaly_type %>%
        select_(
          anomaly_type_local_id = ~local_id,
          anomaly_type = ~fingerprint
        ),
      by = "anomaly_type_local_id"
    ) %>%
    left_join(
      parameter %>%
        select_(
          parameter_local_id = ~local_id,
          parameter = ~fingerprint
        ),
      by = "parameter_local_id"
    ) %>%
    select_(
      ~anomaly_type, ~analysis, ~parameter, ~observation
    ) %>%
    rowwise() %>%
    mutate_(
      fingerprint = ~sha1(c(
        anomaly_type = anomaly_type,
        analysis = analysis,
        observation = observation,
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
  unmatched <- sprintf("
    SELECT
      s.observation
    FROM
      staging.%s AS s
    LEFT JOIN
      public.observation AS po
    ON
      s.observation = po.fingerprint
    WHERE
      s.observation IS NOT NULL AND
      po.id IS NULL
    GROUP BY
      s.observation
    ",
    anomaly.sql
  ) %>%
    dbGetQuery(conn = conn)
  if (nrow(unmatched) > 0) {
    if (clean) {
      dbRollback(conn)
    }
    paste(unmatched$observation, collapse = "; ") %>%
      sprintf(fmt = "observations not in database: %s") %>%
      stop()
  }
  sprintf("
    INSERT INTO public.anomaly
      (fingerprint, anomaly_type, observation, analysis, parameter)
    SELECT
      s.fingerprint,
      pat.id AS anomaly_type,
      po.id AS observation,
      pa.id AS analysis,
      pp.id AS parameter
    FROM
      (
        (
          (
            (
              staging.%s AS s
            INNER JOIN
              public.anomaly_type AS pat
            ON
              s.anomaly_type = pat.fingerprint
            )
          LEFT JOIN
            public.observation AS po
          ON
            s.observation = po.fingerprint
          )
        LEFT JOIN
          public.parameter AS pp
        ON
          s.parameter = pp.fingerprint
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
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("parameter_", hash)))
    dbCommit(conn)
  }

  attr(anomaly, "SQL") <- anomaly.sql
  attr(anomaly, "hash") <- hash
  return(anomaly)
}
