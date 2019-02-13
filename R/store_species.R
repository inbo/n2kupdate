#' store species in the database
#' @param species a data.frame with species metadata. Must contain at least `local_id`, `scientific_name` and `nbn_key`. Other variable names must match the values in `language$code`.
#' @inheritParams store_language
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute select arrange mutate
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbRemoveTable dbSendQuery dbClearResult
#' @importFrom tidyr gather
store_species <- function(species, language, conn, hash, clean = TRUE){
  species <- character_df(species)
  assert_that(
    inherits(conn, "DBIConnection"),
    has_name(species, "scientific_name"),
    has_name(species, "nbn_key"),
    has_name(species, "local_id"),
    are_equal(anyDuplicated(species$scientific_name), 0L),
    are_equal(anyDuplicated(species$nbn_key), 0L),
    are_equal(anyDuplicated(species$local_id), 0L)
  )

  if (missing(hash)) {
    hash <- sha1(list(species, language, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  if (clean) {
    dbBegin(conn)
  }
  language_sql <- tryCatch(
    store_language(
      language = language, hash = hash, conn = conn, clean = FALSE
    ),
    error = function(e){
      if (clean) {
        dbRollback(conn)
      }
      stop(e)
    }
  )

  lang_code <- species %>%
    select(-.data$local_id, -.data$scientific_name, -.data$nbn_key) %>%
    colnames()
  if (!all(lang_code %in% language$code)) {
    if (clean) {
      dbRollback(conn)
    }
    lang_code[!lang_code %in% language$code] %>%
      sprintf(fmt = "'%s'") %>%
      paste(collapse = ", ") %>%
      stop(" is not available is language$code")
  }

  species %>%
    transmute(
      id = NA_integer_,
      .data$scientific_name,
      .data$nbn_key
    ) %>%
    mutate(fingerprint = map_chr(.data$nbn_key, sha1)) %>%
    arrange(.data$fingerprint) -> sp
  dbWriteTable(
    as.data.frame(sp), conn = conn, row.names = FALSE,
    name = c("staging", paste0("species_", hash))
  )
  species_sql <- dbQuoteIdentifier(conn = conn, paste0("species_", hash))
  sprintf("
    INSERT INTO public.species (fingerprint, scientific_name, nbn_key)
    SELECT s.fingerprint, s.scientific_name, s.nbn_key
    FROM staging.%s AS s
    LEFT JOIN public.species AS p ON p.fingerprint = s.fingerprint
    WHERE p.id IS NULL;",
    species_sql
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()
  sprintf("
    UPDATE staging.%1$s AS t
    SET id = p.id
    FROM staging.%1$s AS d
    INNER JOIN public.species AS p ON p.fingerprint = d.fingerprint
    WHERE t.fingerprint = d.fingerprint;",
    species_sql
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()
  sprintf("
    UPDATE public.species AS t
    SET scientific_name = s.scientific_name
    FROM staging.%1$s AS s
    INNER JOIN public.species AS p ON p.id = s.id
    WHERE s.scientific_name <> p.scientific_name AND s.id = t.id",
    species_sql
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()

  sp %>%
    select(-.data$id) %>%
    inner_join(species, by = c("scientific_name", "nbn_key")) %>%
    select(-.data$scientific_name, -.data$nbn_key) %>%
    gather(key = "code", value = "description", lang_code) %>%
    dbWriteTable(
      conn = conn, row.names = FALSE,
      name = c("staging", paste0("species_common_name_", hash))
    )
  common_sql <- dbQuoteIdentifier(conn, paste0("species_common_name_", hash))

  # destroy values which are no longer used
  sprintf("
    UPDATE public.species_common_name AS target
    SET destroy = current_timestamp
    FROM public.species_common_name AS p
    LEFT JOIN (
      staging.%s AS scn
      INNER JOIN staging.%s AS l ON scn.code = l.code
      INNER JOIN staging.%s AS s ON scn.fingerprint = s.fingerprint
    ) ON
      p.language = l.id AND p.species = s.id AND p.description = scn.description
    WHERE
      p.destroy IS NULL AND scn.description IS NULL AND
      p.language = target.language AND p.species = target.species AND
      p.spawn = target.spawn",
    common_sql, language_sql, species_sql
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()
  # insert new values
  sprintf("
    WITH latest AS (
      SELECT language, species, max(spawn) AS ts
      FROM public.species_common_name
      GROUP BY language, species
    )
    INSERT INTO public.species_common_name (language, species, description)
    SELECT
      l.id AS language, s.id AS species, scn.description
    FROM (
      staging.%s AS scn
      INNER JOIN staging.%s AS l ON scn.code = l.code
      INNER JOIN staging.%s AS s ON scn.fingerprint = s.fingerprint)
    LEFT JOIN (
      latest
      INNER JOIN public.species_common_name AS p
        ON latest.language = p.language AND latest.species = p.species AND
        latest.ts = p.spawn)
      ON p.language = l.id AND p.species = s.id
    WHERE p.spawn IS NULL OR p.destroy IS NOT NULL;",
    common_sql, language_sql, species_sql
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("language_", hash)))
    dbRemoveTable(conn, c("staging", paste0("species_", hash)))
    dbRemoveTable(conn, c("staging", paste0("species_common_name_", hash)))
    dbCommit(conn)
  }
  sp <- select(sp, -.data$id)
  attr(sp, "hash") <- hash
  return(sp)
}
