#' store a datasource in the database
#' @param datasource a data.frame with datasource metadata
#' @inheritParams store_connect_method
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_ arrange_
#' @importFrom DBI dbWriteTable dbRemoveTable
#' @details datasource must contain at least the variables description, datasource_type and connect_method.
store_datasource <- function(datasource, conn){
  assert_that(inherits(datasource, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datasource, "description"))
  assert_that(has_name(datasource, "datasource_type"))
  assert_that(has_name(datasource, "connect_method"))

  hash <- sha1(list(datasource, Sys.time()))
  connect_method <- store_connect_method(
    connect_method = datasource$connect_method,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  datasource_type <- store_datasource_type(
    datasource_type = datasource$datasource_type,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  datasource_parameter <- datasource %>%
    select_(~-description, ~-datasource_type, ~-connect_method) %>%
    colnames() %>%
    store_datasource_parameter(
      hash = hash,
      conn = conn,
      clean = FALSE
    )

  datasource %>%
    transmute_(
      id = NA_integer_,
      ~description,
      ~datasource_type,
      ~connect_method
    ) %>%
    arrange_(~datasource_type, ~description) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datasource_", hash)),
      row.names = FALSE
    )
  datasource.sql <- paste0("datasource_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    INSERT INTO public.datasource
      (description, datasource_type, connect_method)
    SELECT
      d.description,
      dt.id AS datasource_type,
      cm.id AS connect_method
    FROM
      (
        (
          staging.%s AS d
        INNER JOIN
          staging.%s AS dt
        ON
          d.datasource_type = dt.description
        )
      INNER JOIN
        staging.%s AS cm
      ON
        d.connect_method = cm.description
      )
    LEFT JOIN
      public.datasource AS p
    ON
      p.description = d.description AND
      p.datasource_type = dt.id
    WHERE
      p.id IS NULL;
    ",
    datasource.sql,
    datasource_type,
    connect_method
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    UPDATE
      staging.%s AS t
    SET
      id = p.id
    FROM
      (
        (
          staging.%s AS d
        INNER JOIN
          staging.%s AS dt
        ON
          d.datasource_type = dt.description
        )
      INNER JOIN
        staging.%s AS cm
      ON
        d.connect_method = cm.description
      )
    INNER JOIN
      public.datasource AS p
    ON
      p.description = d.description AND
      p.datasource_type = dt.id
    WHERE
      t.description = p.description AND
      t.datasource_type = d.datasource_type AND
      t.connect_method = d.connect_method;
    ",
    datasource.sql,
    datasource.sql,
    datasource_type,
    connect_method
  ) %>%
    dbGetQuery(conn = conn)
  stopifnot(
    dbRemoveTable(conn, c("staging", paste0("datasource_", hash))),
    dbRemoveTable(conn, c("staging", paste0("datasource_parameter_", hash))),
    dbRemoveTable(conn, c("staging", paste0("datasource_type_", hash))),
    dbRemoveTable(conn, c("staging", paste0("connect_method_", hash)))
  )
  return(hash)
}
