#' Store language
#' @param language the data.frame with language Must contains code and description. Other variables are ignored. code and description must have unique values.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbSendQuery dbClearResult
#' @importFrom purrr map_chr
store_language <- function(language, hash, conn, clean = TRUE){
  language <- character_df(language)
  assert_that(
    inherits(conn, "DBIConnection"),
    noNA(language),
    has_name(language, "code"),
    has_name(language, "description"),
    is.flag(clean),
    noNA(clean),
    are_equal(anyDuplicated(language$code), 0L),
    are_equal(anyDuplicated(language$description), 0L)
  )
  if (missing(hash)) {
    hash <- sha1(list(language, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }

  language %>%
    transmute(id = NA_integer_, .data$code, .data$description) %>%
    mutate(fingerprint = map_chr(.data$code, sha1)) %>%
    as.data.frame() %>%
    dbWriteTable(conn = conn, name = c("staging", paste0("language_", hash)),
                 row.names = FALSE)
  language <- dbQuoteIdentifier(conn, paste0("language_", hash))
  sprintf("
    INSERT INTO public.language (fingerprint, code, description)
    SELECT s.fingerprint, s.code, s.description
    FROM staging.%s AS s
    LEFT JOIN public.language AS p ON s.fingerprint = p.fingerprint
    WHERE p.id IS NULL",
    language
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()
  sprintf("
    UPDATE staging.%1$s AS t
    SET id = p.id
    FROM staging.%1$s AS s
    INNER JOIN public.language AS p ON s.fingerprint = p.fingerprint
    WHERE t.fingerprint = s.fingerprint",
    language
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()
  sprintf("
    UPDATE public.language AS t
    SET description = s.description
    FROM staging.%s AS s INNER JOIN public.language AS p ON s.id = p.id
    WHERE s.description <> p.description AND t.id = s.id",
    language
  ) %>%
    dbSendQuery(conn = conn) %>%
    dbClearResult()

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("language_", hash)))
    dbCommit(conn)
  }
  return(language)
}
