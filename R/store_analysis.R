#' store source species in the database
#' @param analysis a data.frame with file_fingerprint, model_set_local_id, location_group, species_group, last_year, seed, analysis_version, analysis_date, status and status_fingerprint.
#' @param analysis_relation an optional data.frame with analysis and source_analysis. analysis contains the file_fingerprint of the current analysis. source_analysis contains the file_fingerprint of the parent analysis
#' @inheritParams store_datasource_parameter
#' @inheritParams store_analysis_version
#' @inheritParams store_model_set
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select mutate rowwise inner_join left_join
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable dbBegin dbCommit dbRollback
#' @export
store_analysis <- function(
  analysis,
  model_set,
  analysis_version,
  analysis_relation,
  conn,
  hash,
  clean = TRUE
) {
  analysis <- character_df(analysis)

  assert_that(has_name(analysis, "file_fingerprint"))
  assert_that(has_name(analysis, "model_set_local_id"))
  assert_that(has_name(analysis, "location_group"))
  assert_that(has_name(analysis, "species_group"))
  assert_that(has_name(analysis, "last_year"))
  assert_that(has_name(analysis, "seed"))
  assert_that(has_name(analysis, "analysis_version"))
  assert_that(has_name(analysis, "analysis_date"))
  assert_that(has_name(analysis, "status"))
  assert_that(has_name(analysis, "status_fingerprint"))

  assert_that(noNA(analysis))

  if (anyDuplicated(analysis$file_fingerprint) > 0) {
    stop("Duplicated file_fingerprint")
  }

  assert_that(
    all(
      analysis$analysis_version %in%
        analysis_version@AnalysisVersion$Fingerprint
    )
  )

  if (!missing(analysis_relation)) {
    analysis_relation <- character_df(analysis_relation)

    assert_that(has_name(analysis_relation, "analysis"))
    assert_that(has_name(analysis_relation, "source_analysis"))
    analysis_relation %>%
      select(.data$analysis, .data$source_analysis) -> analysis_relation

    assert_that(noNA(analysis_relation))

    analysis_relation %>%
      anti_join(analysis, by = c("analysis" = "file_fingerprint")) %>%
      nrow() -> aj
    if (aj > 0) {
      stop(
"all 'analysis' in 'analysis_relation' must have matching 'file_fingerprint' in
'analysis'"
      )
    }
    analysis_relation %>%
      filter(.data$analysis == .data$source_analysis) %>%
      nrow() -> self
    if (self > 0) {
      stop("'analysis' cannot have itself as 'source_analysis'")
    }

    if (anyDuplicated(analysis_relation) > 0) {
      stop("Duplicated entries in analysis_relation")
    }
  }

  if (missing(hash)) {
    hash <- sha1(
      list(
        analysis, model_set, analysis_version, as.POSIXct(Sys.time())
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

  staging.model_set <- tryCatch(
    store_model_set(
      model_set = model_set,
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
  assert_that(
    all(analysis$model_set_local_id %in% staging.model_set$local_id)
  )
  tryCatch(
    store_analysis_version(
      analysis_version = analysis_version,
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
  staging.status <- tryCatch(
    store_status(
      status = analysis$status,
      clean = FALSE,
      hash = hash,
      conn = conn
    ),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )
  staging.model_set %>%
    select(.data$local_id, model_set = .data$fingerprint) %>%
    inner_join(
      staging.status %>%
        select(.data$description, status = .data$fingerprint) %>%
        inner_join(
          analysis,
          by = c("description" = "status")
        ),
      by = c("local_id" = "model_set_local_id")
    ) %>%
    select(-.data$local_id) %>%
    mutate(analysis_date = as.POSIXct(.data$analysis_date)) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("analysis_", hash)),
      row.names = FALSE
    )
  analysis.sql <- paste0("analysis_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # insert new analysis
  sprintf("
    INSERT INTO public.analysis
      (
        file_fingerprint, model_set, location_group, species_group, last_year,
        seed, analysis_version, analysis_date, status, status_fingerprint
      )
    SELECT
      sa.file_fingerprint,
      pm.id AS model_set,
      pl.id AS location_group,
      psg.id AS species_group,
      sa.last_year,
      sa.seed,
      pav.id AS analysis_version,
      sa.analysis_date,
      ps.id AS status,
      sa.status_fingerprint
    FROM
      (
        (
          (
            (
              (
                staging.%s AS sa
              INNER JOIN
                public.model_set AS pm
              ON
                sa.model_set = pm.fingerprint
              )
            INNER JOIN
              public.location_group AS pl
            ON
              sa.location_group = pl.fingerprint
            )
          INNER JOIN
            public.species_group AS psg
          ON
            sa.species_group = psg.fingerprint
          )
        INNER JOIN
          public.analysis_version AS pav
        ON
          sa.analysis_version = pav.fingerprint
        )
      INNER JOIN
        public.status AS ps
      ON
        sa.status = ps.fingerprint
      )
    LEFT JOIN
      public.analysis AS pa
    ON
      sa.file_fingerprint = pa.file_fingerprint
    WHERE
      pa.id IS NULL",
    analysis.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (missing(analysis_relation)) {

    if (clean) {
      dbRemoveTable(conn, c("staging", paste0("analysis_", hash)))
      dbRemoveTable(conn, c("staging", paste0("analysis_version_", hash)))
      dbRemoveTable(conn, c("staging", paste0("avrp_", hash)))
      dbRemoveTable(conn, c("staging", paste0("model_set_", hash)))
      dbRemoveTable(conn, c("staging", paste0("model_type_", hash)))
      dbRemoveTable(conn, c("staging", paste0("r_package_", hash)))
      dbRemoveTable(conn, c("staging", paste0("status_", hash)))
      dbCommit(conn)
    }

    return(hash)
  }

  analysis_relation %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("analysis_relation_", hash)),
      row.names = FALSE
    )
  analysis_relation.sql <- paste0("analysis_relation_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    SELECT a.*
    FROM staging.%s AS s
    LEFT JOIN public.analysis AS a ON s.analysis = a.file_fingerprint
    WHERE a.id IS NULL",
    analysis_relation.sql
  ) %>%
    dbGetQuery(conn = conn) -> aj
  if (nrow(aj) > 0) {
    if (clean) {
      dbRollback(conn)
    }
    stop("some source_analysis not yet in the database")
  }
  sprintf("
    INSERT INTO public.analysis_relation
      (
        analysis, source_analysis
      )
    SELECT a.id AS analysis, sa.id AS source_analysis
    FROM staging.%s AS s
    INNER JOIN public.analysis AS a ON s.analysis = a.file_fingerprint
    INNER JOIN public.analysis AS sa ON s.source_analysis = sa.file_fingerprint
    LEFT JOIN public.analysis_relation AS ar
      ON ar.analysis = a.id AND ar.source_analysis = sa.id
    WHERE ar.analysis IS NULL",
    analysis_relation.sql
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
    dbRemoveTable(conn, c("staging", paste0("analysis_relation_", hash)))
    dbCommit(conn)
  }

  return(hash)
}
