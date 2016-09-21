#' store species in the database
#' @param species a data.frame with species metadata. Must contain at least local_id, scientific_name and nbn_key. Other variable names must match the values in language$code..
#' @inheritParams store_language
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_ select_ arrange_  mutate_each_ funs rename_
#' @importFrom DBI dbWriteTable dbRemoveTable
#' @importFrom tidyr gather_
store_species <- function(species, language, conn, hash, clean = TRUE){
  assert_that(inherits(species, "data.frame"))
  assert_that(inherits(language, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(species, "scientific_name"))
  assert_that(has_name(species, "nbn_key"))
  assert_that(has_name(species, "local_id"))
  assert_that(are_equal(anyDuplicated(species$scientific_name), 0L))
  assert_that(are_equal(anyDuplicated(species$nbn_key), 0L))

  lang.code <- species %>%
    select_(~-local_id, ~-scientific_name, ~-nbn_key) %>%
    colnames()
  if (!all(lang.code %in% language$code)) {
    lang.code[!lang.code %in% language$code] %>%
      sprintf(fmt = "'%s'") %>%
      paste(collapse = ", ") %>%
      stop(" is not available is language$code")
  }

  factors <- sapply(species, is.factor)
  if (any(factors)) {
    species <- species %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  if (missing(hash)) {
    hash <- sha1(list(species, language, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  language.sql <- store_language(
    language = language,
    hash = hash,
    conn = conn,
    clean = FALSE
  )

  sp <- species %>%
    transmute_(
      id = NA_integer_,
      ~scientific_name,
      ~nbn_key
    ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(
      scientific_name = scientific_name,
      nbn_key = nbn_key
    ))) %>%
    arrange_(~fingerprint)
  sp %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("species_", hash)),
      row.names = FALSE
    )
  species.sql <- paste0("species_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.species
      (fingerprint, scientific_name, nbn_key)
    SELECT
      s.fingerprint,
      s.scientific_name,
      s.nbn_key
    FROM
      staging.%s AS s
    LEFT JOIN
      public.species AS p
    ON
      p.fingerprint = s.fingerprint
    WHERE
      p.id IS NULL;
    ",
    species.sql
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      staging.%s AS d
    INNER JOIN
      public.species AS p
    ON
      p.fingerprint = d.fingerprint
    WHERE
      t.fingerprint = d.fingerprint;
    ",
    species.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)

  sp %>%
    select_(~-id) %>%
    inner_join(species, by = c("scientific_name", "nbn_key")) %>%
    select_(~-scientific_name, ~-nbn_key) %>%
    gather_(
      key_col = "code",
      value_col = "description",
      gather_cols = lang.code
    ) %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("species_common_name_", hash)),
      row.names = FALSE
    )
  common.sql <- paste0("species_common_name_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # destroy values which are no longer used
  sprintf("
    UPDATE
      public.species_common_name AS target
    SET
      destroy = current_timestamp
    FROM
      public.species_common_name AS p
    LEFT JOIN
      (
        (
          staging.%s AS scn
        INNER JOIN
          staging.%s AS l
        ON
          scn.code = l.code
        )
      INNER JOIN
        staging.%s AS s
      ON
        scn.fingerprint = s.fingerprint
      )
    ON
      p.language = l.id AND
      p.species = s.id AND
      p.description = scn.description
    WHERE
      p.destroy IS NULL AND
      scn.description IS NULL AND
      p.language = target.language AND
      p.species = target.species AND
      p.spawn = target.spawn
    ",
    common.sql,
    language.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)
  # insert new values
  sprintf("
    WITH latest AS
      (
        SELECT
          language,
          species,
          max(spawn) AS ts
        FROM
          public.species_common_name
        GROUP BY
          language, species
      )
    INSERT INTO public.species_common_name
      (language, species, description)
    SELECT
      l.id AS language,
      s.id AS species,
      scn.description
    FROM
      (
        (
          staging.%s AS scn
        INNER JOIN
          staging.%s AS l
        ON
          scn.code = l.code
        )
      INNER JOIN
        staging.%s AS s
      ON
        scn.fingerprint = s.fingerprint
      )
    LEFT JOIN
      (
        latest
      INNER JOIN
        public.species_common_name AS p
      ON
        latest.language = p.language AND
        latest.species = p.species AND
        latest.ts = p.spawn
      )
    ON
      p.language = l.id AND
      p.species = s.id
    WHERE
      p.spawn IS NULL OR
      p.destroy IS NOT NULL;",
    common.sql,
    language.sql,
    species.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    stopifnot(
      dbRemoveTable(conn, c("staging", paste0("language_", hash))),
      dbRemoveTable(conn, c("staging", paste0("species_", hash))),
      dbRemoveTable(conn, c("staging", paste0("species_common_name_", hash)))
    )
  }
  sp %>%
    select_(~-id)
  attr(sp, "hash") <- hash
  return(sp)
}
