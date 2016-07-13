#' store a datafield in the database
#' @param datafield a data.frame with datafield metadata
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that has_name noNA are_equal
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_ select_ arrange_  mutate_each_ funs
#' @importFrom DBI dbWriteTable dbRemoveTable
#' @importFrom tidyr gather_
#' @details datafield must contain the variables hash, datasource, table_name, primary_key and datafield_type. Other variables are ignored.
store_datafield <- function(datafield, conn, hash, clean = TRUE){
  assert_that(inherits(datafield, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datafield, "hash"))
  assert_that(has_name(datafield, "datasource"))
  assert_that(has_name(datafield, "table_name"))
  assert_that(has_name(datafield, "primary_key"))
  assert_that(has_name(datafield, "datafield_type"))

  assert_that(noNA(datafield))

  assert_that(is.integerish(datafield$datasource))

  assert_that(are_equal(anyDuplicated(datafield$hash), 0L))

  if (missing(hash)) {
    hash <- sha1(list(datafield, Sys.time()))
  } else {
    assert_that(is.string(hash))
  }

  factors <- sapply(datafield, is.factor)
  if (any(factors)) {
    datafield <- datafield %>%
      mutate_each_(funs(as.character), vars = names(factors)[factors])
  }

  datafield_type <- store_datafield_type(
    datafield_type = datafield$datafield_type,
    hash = hash,
    conn = conn,
    clean = FALSE
  )

  datafield %>%
    transmute_(
      id = NA_integer_,
      ~hash,
      ~datasource,
      ~table_name,
      ~primary_key,
      ~datafield_type
    ) %>%
    arrange_(~datasource, ~table_name) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datafield_", hash)),
      row.names = FALSE
    )
  datafield.sql <- paste0("datafield_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.datafield
      (datasource, table_name, primary_key, datafield_type)
    SELECT
      d.datasource,
      d.table_name,
      d.primary_key,
      dt.id AS datafield_type
    FROM
      (
        staging.%s AS d
      INNER JOIN
        staging.%s AS dt
      ON
        d.datafield_type = dt.description
      )
    LEFT JOIN
      public.datafield AS p
    ON
      p.datasource = d.datasource AND
      p.table_name = d.table_name AND
      p.primary_key = d.primary_key AND
      p.datafield_type = dt.id
    WHERE
      p.id IS NULL;
    ",
    datafield.sql,
    datafield_type
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      (
        staging.%s AS d
      INNER JOIN
        staging.%s AS dt
      ON
        d.datafield_type = dt.description
      )
    INNER JOIN
      public.datafield AS p
    ON
      p.datasource = d.datasource AND
      p.table_name = d.table_name AND
      p.primary_key = d.primary_key AND
      p.datafield_type = dt.id
    WHERE
      d.hash = t.hash;
    ",
    datafield.sql,
    datafield.sql,
    datafield_type
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    stopifnot(
      dbRemoveTable(conn, c("staging", paste0("datafield_", hash))),
      dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    )
  }
  return(hash)
}
