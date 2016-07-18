#' Store a vector of datafield types
#' @param datafield_type the vector with datafield types.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr data_frame %>% rowwise mutate_ select_
#' @importFrom digest sha1
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_datafield_type <- function(datafield_type, hash, conn, clean = TRUE){
  assert_that(is(datafield_type, "character"))
  assert_that(noNA(datafield_type))
  if (missing(hash)) {
    hash <- sha1(list(datafield_type, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  dft <- data_frame(
    description = sort(unique(datafield_type)),
    id = NA_integer_
  ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(description = description)))
  dft %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datafield_type_", hash)),
      row.names = FALSE
    )
  datafield_type <- paste0("datafield_type_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.datafield_type
      (fingerprint, description)
    SELECT
      s.fingerprint,
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.datafield_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    datafield_type
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
      public.datafield_type AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    datafield_type,
    datafield_type
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
  }

  dft <- dft %>%
    select_(~-id)
  attr(dft, "SQL") <- datafield_type
  return(dft)
}
