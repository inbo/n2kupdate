#' store source species in the database
#' @param source_species a data.frame with source species metadata. Must contain local_id, description, datafield_local_id and extrenal_code. Other variables are ignored.
#' @param datafield a data.frame with datafield metadata. Must contain variables local_id, datasource, table_name, primary_key and datafield_type.
#' @inheritParams store_datasource_parameter
#' @importFrom assertthat assert_that is.string is.flag noNA has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% select_ mutate_ rowwise mutate_each_ funs inner_join left_join transmute_ filter_
#' @importFrom DBI dbQuoteIdentifier dbWriteTable dbGetQuery dbRemoveTable
#' @export
store_source_species <- function(
  source_species,
  datafield,
  conn,
  hash,
  clean = TRUE
) {
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(inherits(source_species, "data.frame"))

  assert_that(has_name(source_species, "local_id"))
  assert_that(has_name(source_species, "description"))
  assert_that(has_name(source_species, "datafield_local_id"))
  assert_that(has_name(source_species, "external_code"))

  assert_that(noNA(select_(source_species)))

  assert_that(are_equal(anyDuplicated(source_species$local_id), 0L))

  dup <- source_species %>%
    select_(~datafield_local_id, ~external_code) %>%
    anyDuplicated()
  if (dup > 0) {
    stop(
"Duplicate combinations of datafield_local_id and external_code are found in
source_species."
    )
  }

  source_species <- as.character(source_species)

  if (missing(hash)) {
    hash <- sha1(list(source_species, datafield, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  tryCatch(
    store_datafield(
      datafield = datafield,
      conn = conn,
      hash = hash,
      clean = FALSE
    ),
    error = function(e){
      c("staging", paste0("datafield_", hash)) %>%
        DBI::dbRemoveTable(conn = conn)
      c("staging", paste0("datafield_type_", hash)) %>%
        DBI::dbRemoveTable(conn = conn)
      stop(e)
    }
  )

  assert_that(all(source_species$datafield_local_id %in% datafield$local_id))
  datafield.sql <- paste0("datafield_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  source_species <- sprintf("
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
    inner_join(source_species, by = "datafield_local_id") %>%
    rowwise() %>%
    mutate_(
      fingerprint = ~sha1(c(
        datafield = datafield,
        external_code = external_code
      ))
    )
  source_species %>%
    transmute_(
      id = NA_integer_,
      ~fingerprint,
      ~description,
      ~datafield_local_id,
      ~external_code
    ) %>%
    arrange_(~fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("source_species_", hash)),
      row.names = FALSE
    )
  source_species.sql <- paste0("source_species_", hash) %>%
    dbQuoteIdentifier(conn = conn)

  # update description for existing rows
  sprintf("
    UPDATE
      public.source_species AS t
    SET
      description = s.description
    FROM
      staging.%s AS s
    INNER JOIN
      public.source_species AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.description != s.description AND
      t.id = p.id;",
    source_species.sql
  ) %>%
    dbGetQuery(conn = conn)

  # store source_speciess
  sprintf("
    INSERT INTO public.source_species
      (fingerprint, description, datafield, external_code)
    SELECT
      s.fingerprint,
      s.description,
      d.id AS datafield,
      s.external_code
    FROM
      (
        staging.%s AS s
      INNER JOIN
        staging.%s AS d
      ON
        s.datafield_local_id = d.local_id
      )
    LEFT JOIN
      public.source_species AS p
    ON
      p.fingerprint = s.fingerprint
    WHERE
      p.id IS NULL;",
    source_species.sql,
    datafield.sql
  ) %>%
    dbGetQuery(conn = conn)
  # update source_species id in staging
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      staging.%s AS s
    INNER JOIN
      public.source_species AS p
    ON
      p.fingerprint = s.fingerprint
    WHERE
      s.fingerprint = t.fingerprint;",
    source_species.sql,
    source_species.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    stopifnot(
      dbRemoveTable(conn, c("staging", paste0("datafield_", hash))),
      dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash))),
      dbRemoveTable(conn, c("staging", paste0("source_species_", hash)))
    )
  }

  attr(source_species, "hash") <- hash
  return(source_species)
}
