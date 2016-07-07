#' Store a vector of connect methods
#' @param connect_method the vector with connect methods.
#' @param hash the hash of the update session
#' @param conn a DBIconnection
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom methods is
#' @importFrom dplyr data_frame %>%
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery
store_connect_method <- function(connect_method, hash, conn){
  assert_that(is(connect_method, "character"))
  assert_that(is.string(hash))
  assert_that(inherits(conn, "DBIConnection"))

  data_frame(
    description = unique(connect_method),
    id = NA_integer_
  ) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("connect_method_", hash)),
      row.names = FALSE
    )
  connect_method <- paste0("connect_method_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.connect_method
      (description)
    SELECT
      s.description
    FROM
      staging.%s AS s
    LEFT JOIN
      public.connect_method AS p
    ON
      s.description = p.description
    WHERE
      p.id IS NULL",
    connect_method
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
      public.connect_method AS p
    ON
      s.description = p.description
    WHERE
      t.description = s.description",
    connect_method,
    connect_method
  ) %>%
    dbGetQuery(conn = conn)
  return(connect_method)
}
