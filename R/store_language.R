#' Store language
#' @param language the data.frame with language Must contains code and description. Other variables are ignored. code and description must have unique values.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% mutate_each_ funs rowwise mutate_
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_language <- function(language, hash, conn, clean = TRUE){
  assert_that(inherits(language, "data.frame"))
  assert_that(noNA(language))
  assert_that(has_name(language, "code"))
  assert_that(has_name(language, "description"))
  if (missing(hash)) {
    hash <- sha1(list(language, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(are_equal(anyDuplicated(language$code), 0L))
  assert_that(are_equal(anyDuplicated(language$description), 0L))

  factors <- sapply(language, is.factor)
  if (any(factors)) {
    language <- language %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  language %>%
    transmute_(
      id = ~NA_integer_,
      ~code,
      ~description
    ) %>%
    rowwise() %>%
    mutate_(fingerprint = ~sha1(c(code = code, description = description))) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("language_", hash)),
      row.names = FALSE
    )
  language <- paste0("language_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.language
      (fingerprint, code, description)
    SELECT
      s.fingerprint,
      s.code,
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.language AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    language
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
      public.language AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      t.fingerprint = s.fingerprint",
    language,
    language
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("language_", hash)))
  }
  return(language)
}
