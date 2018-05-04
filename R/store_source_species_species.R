#' store source species in the database
#' @param source_species_species as data.frame linking the local species id to the local source_species id. Must contain species_local_id and source_species_local_id. Other variables are ignored.
#' @inheritParams store_datasource_parameter
#' @inheritParams store_species
#' @inheritParams store_source_species
#' @inheritParams store_language
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select_ rowwise inner_join left_join transmute_
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable dbBegin dbCommit dbRollback
#' @export
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
  source_species_species <- character_df(source_species_species)

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

  if (missing(hash)) {
    hash <- sha1(
      list(
        species, language, source_species, source_species_species, datafield,
        as.POSIXct(Sys.time())
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

  staging.species <- tryCatch(
    store_species(
      species = species,
      language = language,
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
  staging.source_species <- tryCatch(
    store_source_species(
      source_species = source_species,
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
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbRemoveTable(conn, c("staging", paste0("species_", hash)))
    dbRemoveTable(conn, c("staging", paste0("species_common_name_", hash)))
    dbRemoveTable(conn, c("staging", paste0("language_", hash)))
    dbRemoveTable(conn, c("staging", paste0("source_species_", hash)))
    dbRemoveTable(conn, c("staging", paste0("source_species_species_", hash)))
    dbCommit(conn)
  }

  attr(staging.species, "hash") <- hash
  return(staging.species)
}
