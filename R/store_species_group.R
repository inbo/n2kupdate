#' Store species groups
#' @param species_group the data.frame with species groups. Must contains local_id, description and scheme. Other variables are ignored. local_id must have unique values.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% mutate_each_ funs rowwise mutate_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_species_group <- function(species_group, hash, conn, clean = TRUE){
  species_group <- character_df(species_group)
  assert_that(noNA(species_group))
  assert_that(has_name(species_group, "local_id"))
  assert_that(has_name(species_group, "description"))
  assert_that(has_name(species_group, "scheme"))
  if (missing(hash)) {
    hash <- sha1(list(species_group, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(are_equal(anyDuplicated(species_group$local_id), 0L))


  if (clean) {
    dbBegin(conn)
  }

  staging.species_group <- species_group %>%
    transmute_(
      id = ~NA_integer_,
      ~local_id,
      ~description,
      ~scheme
    ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(description = description)))
  staging.species_group %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("species_group_", hash)),
      row.names = FALSE
    )
  species_group <- paste0("species_group_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.species_group
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
      public.species_group AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    species_group
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
      public.species_group AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    species_group,
    species_group
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("species_group_", hash)))
    dbCommit(conn)
  }

  attr(staging.species_group, "hash") <- hash
  return(staging.species_group)
}
