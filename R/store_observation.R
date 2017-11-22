#' store a datafield in the database
#' @param observation a data.frame with observation metadata. Must contain the variables local_id, datafield_local_id, external_code, location_local_id, year and parameter_local_id. Other variables are ignored. datafield_local_id, external_code and parameter_local_id can be missing.
#' @inheritParams store_datasource_parameter
#' @inheritParams store_datafield
#' @inheritParams store_location
#' @inheritParams store_parameter
#' @export
#' @importFrom assertthat assert_that has_name noNA is.flag are_equal
#' @importFrom digest sha1
#' @importFrom dplyr %>% select_ filter_ anti_join inner_join left_join rowwise mutate_
#' @importFrom DBI dbReadTable dbWriteTable dbGetQuery dbRemoveTable dbQuoteIdentifier
store_observation <- function(
  datafield,
  observation,
  location,
  parameter,
  conn,
  hash,
  clean = TRUE
){
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(inherits(conn, "DBIConnection"))

  observation <- character_df(observation)
  assert_that(has_name(observation, "local_id"))
  assert_that(has_name(observation, "datafield_local_id"))
  assert_that(has_name(observation, "external_code"))
  assert_that(has_name(observation, "location_local_id"))
  assert_that(has_name(observation, "year"))
  assert_that(has_name(observation, "parameter_local_id"))

  assert_that(noNA(select_(observation, ~local_id, ~location_local_id, ~year)))

  assert_that(are_equal(anyDuplicated(observation$local_id), 0L))

  if (any(
    is.na(observation$datafield_local_id) != is.na(observation$external_code)
  )) {
    stop("provide either both datafield and external_code or neither")
  }
  dupl <- observation %>%
    select_(
      ~datafield_local_id,
      ~external_code,
      ~location_local_id,
      ~year,
      ~parameter_local_id
    ) %>%
    anyDuplicated()
  if (dupl > 0) {
    stop("duplicated values in observation")
  }

  if (missing(hash)) {
    hash <- sha1(list(
      observation,
      datafield,
      location,
      parameter,
      as.POSIXct(Sys.time())
    ))
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }

  location_stored <- tryCatch(
    store_location(
      location = location,
      datafield = datafield,
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

  parameter_stored <- tryCatch(
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

  datafield_stored <- dbReadTable(
    conn = conn,
    name = c("staging", paste0("datafield_", hash))
  )

  at <- observation %>%
    anti_join(location_stored, by = c("location_local_id" = "local_id")) %>%
    nrow()
  if (at > 0) {
    stop("each observation must have matching local_id in location")
  }
  observation_stored <- observation %>%
    inner_join(
      location_stored %>%
        select_(
          location_local_id = ~local_id,
          location_fingerprint = ~fingerprint
        ),
      by = "location_local_id"
    )

  if (!all(is.na(observation$datafield_local_id))) {
    at <- observation %>%
      filter_(~!is.na(external_code)) %>%
      anti_join(datafield_stored, by = c("datafield_local_id" = "local_id")) %>%
      nrow()
    if (at > 0) {
      stop("each observation must have matching local_id in datafield")
    }

    observation_stored <- observation_stored %>%
      left_join(
        datafield_stored %>%
          select_(
            datafield_local_id = ~local_id,
            datafield_fingerprint = ~fingerprint
          ),
        by = "datafield_local_id"
      )
  } else {
    observation_stored <- observation_stored %>%
      mutate_(datafield_fingerprint = ~NA_character_)
  }

  if (!all(is.na(observation$parameter_local_id))) {
    at <- observation %>%
      filter_(~!is.na(parameter_local_id)) %>%
      anti_join(parameter_stored, by = c("parameter_local_id" = "local_id")) %>%
      nrow()
    if (at > 0) {
      stop("each observation must have matching local_id in datafield")
    }

    observation_stored <- observation_stored %>%
      left_join(
        parameter_stored %>%
          select_(
            parameter_local_id = ~local_id,
            parameter_fingerprint = ~fingerprint
          ),
        by = "parameter_local_id"
      )
  } else {
    observation_stored <- observation_stored %>%
      mutate_(parameter_fingerprint = ~NA_character_)
  }
  observation_stored <- observation_stored %>%
    rowwise() %>%
    mutate_(
      fingerprint = ~sha1(c(
        datafield = datafield_fingerprint,
        external_code = external_code,
        location = location_fingerprint,
        year = year,
        parameter = parameter_fingerprint
      ))
    )
  observation_stored %>%
    select_(
      ~local_id,
      ~fingerprint,
      datafield = ~datafield_fingerprint,
      ~external_code,
      location = ~location_fingerprint,
      ~year,
      parameter = ~parameter_fingerprint
    ) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("observation_", hash)),
    )

  observation.sql <- paste0("observation_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  location.sql <- paste0("location_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  datafield.sql <- paste0("datafield_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  sprintf("
    INSERT INTO public.observation
      (fingerprint, datafield, external_code, location, year, parameter)
    SELECT
      s.fingerprint AS fingerprint,
      d.id AS datafield,
      s.external_code AS external_code,
      l.id AS location,
      s.year AS year,
      pm.id AS parameter
    FROM
      (
        (
          (
            staging.%s AS s
          INNER JOIN
            staging.%s AS l
          ON
            s.location = l.fingerprint
          )
        LEFT JOIN
          staging.%s AS d
        ON
          s.datafield = d.fingerprint
        )
      LEFT JOIN
        public.parameter AS pm
      ON
        s.parameter = pm.fingerprint
      )
    LEFT JOIN
      public.observation AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL;",
    observation.sql,
    location.sql,
    datafield.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("location_", hash)))
    dbRemoveTable(conn, c("staging", paste0("parameter_", hash)))
    dbRemoveTable(conn, c("staging", paste0("observation_", hash)))
    dbCommit(conn)
  }

  observation_stored <- observation_stored %>%
    select_(~local_id, ~fingerprint)
  attr(observation_stored, "hash") <- hash
  return(observation_stored)
}
