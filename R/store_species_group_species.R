#' store all species related information in the database
#' @param species_group_species as data.frame linking the local species group id to the local species id. Must contain variables species_local_id and species_group_local_id. Other variables are ignored.
#' @inheritParams store_datasource_parameter
#' @inheritParams store_species
#' @inheritParams store_source_species
#' @inheritParams store_language
#' @inheritParams store_source_species_species
#' @inheritParams store_species_group
#' @export
#' @importFrom assertthat assert_that has_name noNA
#' @importFrom dplyr %>% select inner_join
#' @importFrom digest sha1
#' @importFrom DBI dbBegin dbCommit dbRollback
store_species_group_species <- function(
  species,
  language,
  source_species,
  source_species_species,
  datafield,
  species_group,
  species_group_species,
  hash,
  conn,
  clean = TRUE
){
  species_group_species <- character_df(species_group_species)

  assert_that(has_name(species_group_species, "species_group_local_id"))
  assert_that(has_name(species_group_species, "species_local_id"))

  assert_that(noNA(species_group_species))

  dup <- species_group_species %>%
    select(.data$species_local_id, .data$species_group_local_id) %>%
    anyDuplicated()
  if (dup > 0) {
    stop(
"Duplicate combinations of species_local_id and species_group_local_id are
found in species_group_species."
    )
  }

  if (missing(hash)) {
    hash <- sha1(
      list(
        species, language, source_species, source_species_species, datafield,
        species_group, species_group_species, as.POSIXct(Sys.time())
      )
    )
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }
  staging.species <- tryCatch(
    store_source_species_species(
      species = species,
      language = language,
      source_species = source_species,
      source_species_species = source_species_species,
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
  staging.species_group <- tryCatch(
    store_species_group(
      species_group = species_group,
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

  assert_that(
    all(species_group_species$species_local_id %in% species$local_id)
  )
  assert_that(
    all(
      species_group_species$species_group_local_id %in%
        species_group$local_id
    )
  )

  staging.species %>%
    inner_join(
      species,
      by = c("scientific_name", "nbn_key")
    ) %>%
    select(
      species_local_id = .data$local_id,
      species_fingerprint = .data$fingerprint
    ) %>%
    inner_join(
      species_group_species,
      by = "species_local_id"
    ) %>%
    inner_join(
      staging.species_group %>%
        select(
          species_group_local_id = .data$local_id,
          species_group_fingerprint = .data$fingerprint
        ),
      by = c("species_group_local_id")
    ) %>%
    select(.data$species_fingerprint, .data$species_group_fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("species_group_species_", hash)),
      row.names = FALSE
    )
  species_group_species.sql <- paste0("species_group_species_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  species_group.sql <- paste0("species_group_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  species.sql <- paste0("species_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # destroy values which are no longer used
  sprintf("
    UPDATE
      public.species_group_species AS t
    SET
      destroy = current_timestamp
    FROM
      public.species_group_species AS p
    LEFT JOIN
      (
        (
          staging.%s AS sss
        INNER JOIN
          staging.%s AS ss
        ON
          sss.species_group_fingerprint = ss.fingerprint
        )
      INNER JOIN
        staging.%s AS s
      ON
        sss.species_fingerprint = s.fingerprint
      )
    ON
      p.species = s.id AND
      p.species_group = ss.id
    WHERE
      p.destroy IS NULL AND
      s.id IS NULL AND
      p.species = t.species AND
      p.species_group = t.species_group AND
      p.spawn = t.spawn;
    ",
    species_group_species.sql,
    species_group.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)
  # insert new values
  sprintf("
    WITH latest AS
      (
        SELECT
          species_group,
          species,
          max(spawn) AS ts
        FROM
          public.species_group_species
        GROUP BY
          species_group, species
      )
    INSERT INTO public.species_group_species
      (species_group, species)
    SELECT
      ss.id AS species_group,
      s.id AS species
    FROM
      (
        (
          staging.%s AS sss
        INNER JOIN
          staging.%s AS ss
        ON
          sss.species_group_fingerprint = ss.fingerprint
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
        public.species_group_species AS p
      ON
        latest.species_group = p.species_group AND
        latest.species = p.species AND
        latest.ts = p.spawn
      )
    ON
      p.species_group = ss.id AND
      p.species = s.id
    WHERE
      p.spawn IS NULL OR
      p.destroy IS NOT NULL;",
    species_group_species.sql,
    species_group.sql,
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
    dbRemoveTable(conn, c("staging", paste0("species_group_", hash)))
    dbRemoveTable(conn, c("staging", paste0("species_group_species_", hash)))
    dbCommit(conn)
  }

  output <- list(
    species = staging.species,
    species_group = staging.species_group
  )
  attr(output, "hash") <- hash

  return(output)
}
