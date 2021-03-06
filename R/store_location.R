#' store locations in the database
#' @param location a data.frame with location metadata. Must contain the following columns: local_id, description, parent_local_id, datafield_local_id and external_code. Other columns are ignored.
#' @inheritParams store_datafield
#' @inheritParams store_datasource_parameter
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select mutate rowwise inner_join left_join transmute arrange filter
#' @importFrom rlang .data
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable
#' @importFrom purrr pmap map_chr
#' @export
#' @details
#'
#' \itemize{
#'  \item location must have variables local_id, description, parent_local_id, datafield_local_id and extranal_code. Other variables are ignored
#'  \item datafield must have variables local_id, datasource, table_name, primary_key and datafield_type
#'  \item all local_id variables must be unique within their data.frame
#'  \item all values in location$datafield_local_id must exist in datafield$local_id
#'  \item all values in location$parent_location must be either NA or exist in location$local_id
#' }
store_location <- function(location, datafield, conn, hash, clean = TRUE) {
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  location <- character_df(location)

  assert_that(has_name(location, "local_id"))
  assert_that(has_name(location, "description"))
  assert_that(has_name(location, "parent_local_id"))
  assert_that(has_name(location, "datafield_local_id"))
  assert_that(has_name(location, "external_code"))

  assert_that(noNA(select(location, -.data$parent_local_id)))

  assert_that(are_equal(anyDuplicated(location$local_id), 0L))

  dup <- location %>%
    select(
      .data$datafield_local_id,
      .data$external_code,
      .data$parent_local_id
    ) %>%
    anyDuplicated()
  if (dup > 0) {
    stop(
"Duplicate combinations of datafield_local_id, external_code and
parent_local_id are found in location."
    )
  }

  assert_that(all(location$datafield_local_id %in% datafield$local_id))

  assert_that(
    all(
      is.na(location$parent_local_id) |
      location$parent_local_id %in% location$local_id
    )
  )

  if (missing(hash)) {
    hash <- sha1(list(location, datafield, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }
  tryCatch(
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

  datafield.sql <- paste0("datafield_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    SELECT
      df.local_id AS datafield_local_id,
      df.fingerprint AS datafield
    FROM
      staging.%s AS df
    INNER JOIN
      (
        public.datasource AS d
      INNER JOIN
        public.datasource_type AS dt
      ON
        d.datasource_type = dt.id
      )
    ON
      df.datasource = d.fingerprint",
    datafield.sql
  ) %>%
    dbGetQuery(conn = conn) %>%
    inner_join(location, by = "datafield_local_id") %>%
    mutate(
      parent_fingerprint = NA,
      fingerprint = pmap(
        list(
          parent_fingerprint = .data$parent_fingerprint,
          datafield = .data$datafield,
          external_code = .data$external_code
        ),
        c
      ) %>%
        map_chr(sha1),
      fingerprint = ifelse(
        is.na(.data$parent_local_id),
        .data$fingerprint,
        NA
      )
    ) -> location
  repeat {
    location %>%
      left_join(
        location %>%
          filter(!is.na(.data$fingerprint)) %>%
          select(
            parent_local_id = .data$local_id,
            new_parent = .data$fingerprint
          ),
        by = "parent_local_id"
      ) %>%
      mutate(
        parent_fingerprint = ifelse(
          is.na(.data$parent_fingerprint),
          .data$new_parent,
          .data$parent_fingerprint
        ),
        new_fingerprint = pmap(
          list(
            parent_fingerprint = .data$parent_fingerprint,
            datafield = .data$datafield,
            external_code = .data$external_code
          ),
          c
        ) %>%
          map_chr(sha1),
        fingerprint = ifelse(
          is.na(.data$fingerprint) & !is.na(.data$parent_fingerprint),
          .data$new_fingerprint,
          .data$fingerprint
        ),
      ) %>%
      select(-.data$new_parent, -.data$new_fingerprint) -> location
    if (isTRUE(all.equal(
      is.na(location$parent_local_id),
      is.na(location$parent_fingerprint)
    ))) {
      break
    }
  }
  location %>%
    transmute(
      id = NA_integer_,
      .data$fingerprint,
      .data$description,
      parent_location = NA_integer_,
      .data$parent_fingerprint,
      .data$datafield_local_id,
      .data$external_code
    ) %>%
    arrange(.data$fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("location_", hash)),
      row.names = FALSE
    )
  location.sql <- paste0("location_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # update description for existing rows
  sprintf("
    UPDATE
      public.location AS t
    SET
      description = s.description
    FROM
      staging.%s AS s
    INNER JOIN
      public.location AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.description != s.description AND
      t.id = p.id;",
    location.sql
  ) %>%
    dbGetQuery(conn = conn)

  repeat {
    # store locations
    sprintf("
      INSERT INTO public.location
        (fingerprint, description, parent_location, datafield, external_code)
      SELECT
        l.fingerprint,
        l.description,
        l.parent_location,
        d.id AS datafield,
        l.external_code
      FROM
        (
          staging.%s AS l
        INNER JOIN
          staging.%s AS d
        ON
          l.datafield_local_id = d.local_id
        )
      LEFT JOIN
        public.location AS p
      ON
        p.fingerprint = l.fingerprint
      WHERE
        (
          l.parent_fingerprint IS NULL OR
          l.parent_location IS NOT NULL
        ) AND
        p.id IS NULL;",
      location.sql,
      datafield.sql
    ) %>%
      dbGetQuery(conn = conn)
    # update location id in staging
    sprintf("
      UPDATE
        staging.%s AS t
      SET
        id = p.id
      FROM
        staging.%s AS l
      INNER JOIN
        public.location AS p
      ON
        p.fingerprint = l.fingerprint
      WHERE
        l.fingerprint = t.fingerprint;",
      location.sql,
      location.sql
    ) %>%
      dbGetQuery(conn = conn)
    # update parent_location in staging
    sprintf("
      UPDATE
        staging.%s AS t
      SET
        parent_location = p.id
      FROM
        staging.%s AS p
      INNER JOIN
        staging.%s AS c
      ON
        p.fingerprint = c.parent_fingerprint
      WHERE
        p.id IS NOT NULL AND
        c.parent_location IS NULL AND
        c.fingerprint = t.fingerprint;",
      location.sql,
      location.sql,
      location.sql
    ) %>%
      dbGetQuery(conn = conn)
    to.do <- sprintf("
      SELECT
        COUNT(*)
      FROM
        staging.%s
      WHERE
        id IS NULL;",
      location.sql
    ) %>%
      dbGetQuery(conn = conn)
    if (to.do$count == 0) {
      break
    }
  }

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("location_", hash)))
    dbCommit(conn)
  }

  attr(location, "hash") <- hash
  return(location)
}
