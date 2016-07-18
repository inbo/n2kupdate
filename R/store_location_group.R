#' Store location groups
#' @param location_group the data.frame with location groups. Must contains local_id, description and scheme. Other variables are ignored. local_id must have unique values.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% mutate_each_ funs
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_location_group <- function(location_group, hash, conn, clean = TRUE){
  assert_that(inherits(location_group, "data.frame"))
  assert_that(noNA(location_group))
  assert_that(has_name(location_group, "local_id"))
  assert_that(has_name(location_group, "description"))
  assert_that(has_name(location_group, "scheme"))
  if (missing(hash)) {
    hash <- sha1(list(location_group, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  assert_that(are_equal(anyDuplicated(location_group$local_id), 0L))

  factors <- sapply(location_group, is.factor)
  if (any(factors)) {
    location_group <- location_group %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  location_group %>%
    transmute_(
      id = ~NA_integer_,
      ~local_id,
      ~description,
      ~scheme
    ) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("location_group_", hash)),
      row.names = FALSE
    )
  location_group <- paste0("location_group_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.location_group
      (description, scheme)
    SELECT
      s.description,
      s.scheme
    FROM
      staging.%s AS s
    LEFT JOIN
      public.location_group AS p
    ON
      s.description = p.description AND
      s.scheme = p.scheme
    WHERE
      p.id IS NULL",
    location_group
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
      public.location_group AS p
    ON
      s.description = p.description AND
      s.scheme = p.scheme
    WHERE
      t.description = s.description AND
      t.scheme = s.scheme",
    location_group,
    location_group
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("location_group_", hash)))
  }
  return(location_group)
}
