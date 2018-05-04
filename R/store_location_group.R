#' Store location groups
#' @param location_group the data.frame with location groups. Must contains local_id, description and scheme. Other variables are ignored. local_id must have unique values.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate transmute
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_location_group <- function(location_group, hash, conn, clean = TRUE){
  location_group <- character_df(location_group)
  assert_that(noNA(location_group))
  assert_that(has_name(location_group, "local_id"))
  assert_that(has_name(location_group, "description"))
  assert_that(has_name(location_group, "scheme"))
  if (missing(hash)) {
    hash <- sha1(list(location_group, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(are_equal(anyDuplicated(location_group$local_id), 0L))

  if (clean) {
    dbBegin(conn)
  }

  staging <- location_group %>%
    transmute(
      id = NA_integer_,
      .data$local_id,
      .data$description,
      .data$scheme
    ) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(scheme = .data$scheme, description = .data$description)))
  staging %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("location_group_", hash)),
      row.names = FALSE
    )
  location_group.sql <- paste0("location_group_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.location_group
      (fingerprint, description, scheme)
    SELECT
      s.fingerprint,
      s.description,
      ps.id AS scheme
    FROM
      (
        staging.%s AS s
      INNER JOIN
        public.scheme AS ps
      ON
        s.scheme = ps.fingerprint
      )
    LEFT JOIN
      public.location_group AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    location_group.sql
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      staging.%s AS s
    INNER JOIN
      public.location_group AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    location_group.sql,
    location_group.sql
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("location_group_", hash)))
    dbCommit(conn)
  }

  attr(staging, "hash") <- hash
  attr(staging, "SQL") <- location_group.sql
  return(staging)
}
