#' store locations in the database
#' @param location a data.frame with location metadata
#' @param datafield a data.frame with datafield metadata
#' @inheritParams store_datasource_parameter
#' @importFrom assertthat assert_that
#' @importFrom digest sha1
#' @export
#' @details
#'
#' \itemize{
#'  \item location mush have variables hash, description, parent_hash, datafield_hash and extranal_code. Other variables are ignored
#'  \item datafield mush have variables hash, datasource, table_name, primary_key and datafield_type
#'  \item all hash variables must be unique within their data.frame
#'  \item all values in location$datafield_hash must exist in datafield$hash
#'  \item all values in location$parent_location must be either NA or exist in location$hash
#' }
store_location <- function(location, datafield, conn, hash, clean = FALSE) {
  if (missing(hash)) {
    hash <- sha1(list(location, datafield, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  store_datafield(
    datafield = datafield,
    conn = conn,
    hash = hash,
    clean = FALSE
  )

  assert_that(inherits(location, "data.frame"))

  assert_that(has_name(location, "hash"))
  assert_that(has_name(location, "description"))
  assert_that(has_name(location, "parent_hash"))
  assert_that(has_name(location, "datafield_hash"))
  assert_that(has_name(location, "external_code"))

  assert_that(noNA(select_(location, ~-parent_hash)))

  assert_that(are_equal(anyDuplicated(location$hash), 0L))

  dup <- location %>%
    select_(~datafield_hash, ~external_code, ~parent_hash) %>%
    anyDuplicated()
  if (dup > 0) {
    stop(
"Duplicate combinations of datafield_hash, external_code and parent_hash are
found in location."
    )
  }

  factors <- sapply(location, is.factor)
  if (any(factors)) {
    location <- location %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }
  assert_that(all(location$datafield_hash %in% datafield$hash))

  assert_that(
    all(
      is.na(location$parent_hash) |
      location$parent_hash %in% location$hash
    )
  )

  location %>%
    transmute_(
      id = NA_integer_,
      ~hash,
      ~description,
      parent_location = NA_integer_,
      ~parent_hash,
      ~datafield_hash,
      ~external_code
    ) %>%
    arrange_(~datafield_hash, ~external_code) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("location_", hash)),
      row.names = FALSE
    )
  location.sql <- paste0("location_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  datafield.sql <- paste0("datafield_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  repeat {
    # store locations
    sprintf("
      INSERT INTO public.location
        (description, parent_location, datafield, external_code)
      SELECT
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
          l.datafield_hash = d.hash
        )
      LEFT JOIN
        public.location AS p
      ON
        p.datafield = d.id AND
        p.external_code = l.external_code AND
        p.parent_location = l.parent_location
      WHERE
        (
          l.parent_hash IS NULL OR
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
        (
          staging.%s AS l
        INNER JOIN
          staging.%s AS d
        ON
          l.datafield_hash = d.hash
        )
      INNER JOIN
        public.location AS p
      ON
        p.datafield = d.id AND
        p.external_code = l.external_code AND
        (
          p.parent_location = l.parent_location OR
          (p.parent_location IS NULL AND l.parent_location IS NULL)
        )
      WHERE
        l.hash = t.hash;",
      location.sql,
      location.sql,
      datafield.sql
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
        p.hash = c.parent_hash
      WHERE
        p.id IS NOT NULL AND
        c.parent_location IS NULL AND
        c.hash = t.hash;",
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
    stopifnot(
      dbRemoveTable(conn, c("staging", paste0("datafield_", hash))),
      dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash))),
      dbRemoveTable(conn, c("staging", paste0("location_", hash)))
    )
  }

  return(hash)
}
