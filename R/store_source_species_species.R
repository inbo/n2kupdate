#' store source species in the database
#' @param source_species_species as data.frame linking the local species id to the local source_species id
#' @inheritParams store_datasource_parameter
#' @inheritParams store_species
#' @inheritParams store_source_species
#' @inheritParams store_language
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select_ mutate_ rowwise mutate_each_ funs inner_join left_join transmute_ filter_
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable
#' @export
#' @details
#'
#' \itemize{
#'  \item species must contain at least the variables scientific_name and nbn_key. Other variables must be listed in language$code.
#'  \item source_species must have variables local_id, description, datafield_local_id and extranal_code. Other variables are ignored.
#'  \item datafield must have variables local_id, datasource, table_name, primary_key and datafield_type
#'  \item all local_id variables must be unique within their data.frame
#'  \item all values in source_species$datafield_local_id must exist in datafield$local_id
#' }
store_source_species_species <- function(
  species,
  language,
  source_species,
  source_species_species,
  datafield,
  conn,
  hash,
  clean = TRUE
) {
  assert_that(inherits(source_species_species, "data.frame"))

  assert_that(has_name(source_species_species, "source_species_local_id"))
  assert_that(has_name(source_species_species, "species_local_id"))

  assert_that(noNA(select_(source_species_species)))

  dup <- source_species_species %>%
    select_(~species_local_id, ~source_species_local_id) %>%
    anyDuplicated()
  if (dup > 0) {
    stop(
"Duplicate combinations of species_local_id and source_species_local_id are
found in source_species_species."
    )
  }

  factors <- sapply(source_species_species, is.factor)
  if (any(factors)) {
    source_species_species <- source_species_species %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  if (missing(hash)) {
    hash <- sha1(
      list(
        species, language, source_species, source_species_species, datafield,
        Sys.time()
      )
    )
  } else {
    assert_that(is.string(hash))
  }

  staging.species <- store_species(
    species = species,
    language = language,
    conn = conn,
    hash = hash,
    clean = FALSE
  )
  staging.source_species <- store_source_species(
    source_species = source_species,
    datafield = datafield,
    conn = conn,
    hash = hash,
    clean = FALSE
  )

  assert_that(
    all(source_species_species$species_local_id %in% species$local_id)
  )
  assert_that(
    all(
      source_species_species$source_species_local_id %in%
        source_species$local_id
    )
  )
  staging.species %>%
    select_(~scientific_name, ~nbn_key, ~fingerprint) %>%
    inner_join(
      species %>%
        select_(~scientific_name, ~nbn_key, ~local_id),
      by = c("scientific_name", "nbn_key")
    ) %>%
    select_(
      species_local_id = ~local_id,
      species_fingerprint = ~fingerprint
    ) %>%
    inner_join(
      source_species_species,
      by = "species_local_id"
    ) %>%
    inner_join(
        staging.source_species %>%
          select_(
            source_species_local_id = ~local_id,
            source_species_fingerprint = ~fingerprint
          ),
      by = "source_species_local_id"
    ) %>%
    select_(~species_fingerprint, ~source_species_fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("source_species_species_", hash)),
      row.names = FALSE
    )
  source_species_species.sql <- paste0("source_species_species_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  source_species.sql <- paste0("source_species_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  species.sql <- paste0("species_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # destroy values which are no longer used
  sprintf("
    UPDATE
      public.source_species_species AS t
    SET
      destroy = current_timestamp
    FROM
      public.source_species_species AS p
    LEFT JOIN
      (
        (
          staging.%s AS sss
        INNER JOIN
          staging.%s AS ss
        ON
          sss.source_species_fingerprint = ss.fingerprint
        )
      INNER JOIN
        staging.%s AS s
      ON
        sss.species_fingerprint = s.fingerprint
      )
    ON
      p.species = s.id AND
      p.source_species = ss.id
    WHERE
      p.destroy IS NULL AND
      s.id IS NULL AND
      p.species = t.species AND
      p.source_species = t.source_species AND
      p.spawn = t.spawn;
    ",
    source_species_species.sql,
    source_species.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)
  # insert new values
  sprintf("
    WITH latest AS
      (
        SELECT
          source_species,
          species,
          max(spawn) AS ts
        FROM
          public.source_species_species
        GROUP BY
          source_species, species
      )
    INSERT INTO public.source_species_species
      (source_species, species)
    SELECT
      ss.id AS source_species,
      s.id AS species
    FROM
      (
        (
          staging.%s AS sss
        INNER JOIN
          staging.%s AS ss
        ON
          sss.source_species_fingerprint = ss.fingerprint
        )
      INNER JOIN
        staging.%s AS s
      ON
        sss.species_fingerprint = s.fingerprint
      )
    LEFT JOIN
      (
        latest
      INNER JOIN
        public.source_species_species AS p
      ON
        latest.source_species = p.source_species AND
        latest.species = p.species AND
        latest.ts = p.spawn
      )
    ON
      p.source_species = ss.id AND
      p.species = s.id
    WHERE
      p.spawn IS NULL OR
      p.destroy IS NOT NULL;",
    source_species_species.sql,
    source_species.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    stopifnot(
      dbRemoveTable(conn, c("staging", paste0("datafield_", hash))),
      dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash))),
      dbRemoveTable(conn, c("staging", paste0("species_", hash))),
      dbRemoveTable(conn, c("staging", paste0("species_common_name_", hash))),
      dbRemoveTable(conn, c("staging", paste0("language_", hash))),
      dbRemoveTable(conn, c("staging", paste0("source_species_", hash))),
      dbRemoveTable(conn, c("staging", paste0("source_species_species_", hash)))
    )
  }

  return(hash)
}
