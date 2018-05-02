#' store the link between locations and location groups in the database
#' @param location_group_location a data.frame with the locations per location group
#' @inheritParams store_datasource_parameter
#' @inheritParams store_location
#' @inheritParams store_location_group
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select_ mutate_ rowwise inner_join left_join transmute_ filter_
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable
#' @export
#' @details
#'
#' \itemize{
#'  \item location_group_location must have variables location_local_id and location_group_local_id.
#'  \item location_group must have variables local_id, description and scheme
#'  \item location must have variables local_id, description, parent_local_id, datafield_local_id and extranal_code. Other variables are ignored
#'  \item datafield must have variables local_id, datasource, table_name, primary_key and datafield_type
#'  \item all local_id variables must be unique within their data.frame
#'  \item all values in location$datafield_local_id must exist in datafield$local_id
#'  \item all values in location$parent_location must be either NA or exist in location$local_id
#'  \item all values in location_group_location$location_local_id must exist in location$local_id
#'  \item all values in location_group_location$location_group_local_id must exist in location_group$local_id
#' }
store_location_group_location <- function(
  location_group_location,
  location_group,
  location,
  datafield,
  conn,
  hash,
  clean = TRUE
) {
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  location_group_location <- character_df(location_group_location)

  assert_that(has_name(location_group_location, "location_local_id"))
  assert_that(has_name(location_group_location, "location_group_local_id"))

  assert_that(noNA(location_group_location))

  assert_that(are_equal(anyDuplicated(location_group_location), 0L))

  if (missing(hash)) {
    hash <- sha1(list(
      location_group_location,
      location_group,
      location,
      datafield,
      as.POSIXct(Sys.time())
    ))
  } else {
    assert_that(is.string(hash))
  }
  if (clean) {
    dbBegin(conn)
  }
  location <- tryCatch(
    store_location(
      location = location,
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
  location.sql <- paste0("location_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  staging.location_group <- store_location_group(
    location_group = location_group,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  location_group.sql <- attr(staging.location_group, "SQL")

  assert_that(
    all(location_group_location$location_local_id %in% location$local_id)
  )
  assert_that(
    all(
      location_group_location$location_group_local_id %in%
        location_group$local_id
    )
  )

  # write to staging table
  location %>%
    select_(location_local_id = ~local_id, ~fingerprint) %>%
    inner_join(location_group_location, by = "location_local_id") %>%
    transmute_(
      location = NA_integer_,
      location_group = NA_integer_,
      location_fingerprint = ~fingerprint,
      ~location_group_local_id
    ) %>%
    arrange_(~location_group_local_id, ~location_fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("lgl_", hash)),
      row.names = FALSE
    )
  location_group_location.sql <- paste0("lgl_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # destroy values which are no longer used
  sprintf("
    UPDATE
      public.location_group_location AS t
    SET
      destroy = current_timestamp
    FROM
      public.location_group_location AS p
    LEFT JOIN
      (
        (
          staging.%s AS lgl
        INNER JOIN
          staging.%s AS lg
        ON
          lgl.location_group_local_id = lg.local_id
        )
      INNER JOIN
        staging.%s AS l
      ON
        lgl.location_fingerprint = l.fingerprint
      )
    ON
      p.location = l.id AND
      p.location_group = lg.id
    WHERE
      p.destroy IS NULL AND
      l.id IS NULL AND
      p.location = t.location AND
      p.location_group = t.location_group AND
      p.spawn = t.spawn;
    ",
    location_group_location.sql,
    location_group.sql,
    location.sql
  ) %>%
    dbGetQuery(conn = conn)
  # insert new values
  sprintf("
    WITH latest AS
      (
        SELECT
          location_group,
          location,
          max(spawn) AS ts
        FROM
          public.location_group_location
        GROUP BY
          location_group, location
      )
    INSERT INTO public.location_group_location
      (location_group, location)
    SELECT
      lg.id AS location_group,
      l.id AS location
    FROM
      (
        (
          staging.%s AS lgl
        INNER JOIN
          staging.%s AS lg
        ON
          lgl.location_group_local_id = lg.local_id
        )
      INNER JOIN
        staging.%s AS l
      ON
        lgl.location_fingerprint = l.fingerprint
      )
    LEFT JOIN
      (
        latest
      INNER JOIN
        public.location_group_location AS p
      ON
        latest.location_group = p.location_group AND
        latest.location = p.location AND
        latest.ts = p.spawn
      )
    ON
      p.location_group = lg.id AND
      p.location = l.id
    WHERE
      p.spawn IS NULL OR
      p.destroy IS NOT NULL;",
    location_group_location.sql,
    location_group.sql,
    location.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("location_", hash)))
    dbRemoveTable(conn, c("staging", paste0("location_group_", hash)))
    dbRemoveTable(conn, c("staging", paste0("lgl_", hash)))
    dbCommit(conn)
  }

  return(staging.location_group)
}
