#' store a datafield in the database
#' @param datafield a data.frame with datafield metadata. Must contain the variables local_id, datasource, table_name, primary_key and datafield_type. Other variables are ignored.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that has_name noNA are_equal is.flag
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ select arrange mutate
#' @importFrom rlang .data
#' @importFrom DBI dbWriteTable dbGetQuery dbRemoveTable dbQuoteIdentifier
#' @importFrom tidyr gather_
store_datafield <- function(datafield, conn, hash, clean = TRUE){
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  datafield <- character_df(datafield)
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datafield, "local_id"))
  assert_that(has_name(datafield, "datasource"))
  assert_that(has_name(datafield, "table_name"))
  assert_that(has_name(datafield, "primary_key"))
  assert_that(has_name(datafield, "datafield_type"))

  assert_that(noNA(datafield))

  assert_that(are_equal(anyDuplicated(datafield$local_id), 0L))

  if (missing(hash)) {
    hash <- sha1(list(datafield, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }

  if (clean) {
    dbBegin(conn)
  }

  datafield_type <- tryCatch(
    store_datafield_type(
      datafield_type = datafield$datafield_type,
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

  df <- datafield %>%
    transmute_(
      id = NA_integer_,
      ~local_id,
      ~datasource,
      ~table_name,
      ~primary_key,
      dft = ~datafield_type
    ) %>%
    inner_join(
      datafield_type %>%
        select(dft = .data$description, datafield_type = .data$fingerprint),
      by = "dft"
    ) %>%
    select(-.data$dft) %>%
    rowwise() %>%
    mutate(fingerprint = sha1(c(
      datasource = .data$datasource,
      table_name = .data$table_name,
      primary_key = .data$primary_key,
      datafield_type = .data$datafield_type
    )))
  df %>%
    arrange(.data$fingerprint) %>%
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
      (fingerprint, datasource, table_name, primary_key, datafield_type)
    SELECT
      d.fingerprint,
      pd.id AS datasource,
      d.table_name,
      d.primary_key,
      dt.id AS datafield_type
    FROM
      (
        (
          staging.%s AS d
        INNER JOIN
          staging.%s AS dt
        ON
          d.datafield_type = dt.fingerprint
        )
      INNER JOIN
        public.datasource AS pd
      ON
        d.datasource = pd.fingerprint
      )
    LEFT JOIN
      public.datafield AS p
    ON
      p.fingerprint = d.fingerprint
    WHERE
      p.id IS NULL;
    ",
    datafield.sql,
    attr(datafield_type, "SQL")
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
      public.datafield AS p
    ON
      p.fingerprint = d.fingerprint
    WHERE
      d.fingerprint = t.fingerprint;
    ",
    datafield.sql,
    datafield.sql
  ) %>%
    dbGetQuery(conn = conn)

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("datafield_", hash)))
    dbRemoveTable(conn, c("staging", paste0("datafield_type_", hash)))
    dbCommit(conn)
  }

  df <- df %>%
    select(-.data$id)
  attr(df, "hash") <- hash
  return(df)
}
