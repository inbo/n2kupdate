#' store a datasource in the database
#' @param datasource a data.frame with datasource metadata
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that has_name
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute_ distinct_ select_ arrange_
#' @importFrom DBI dbWriteTable dbRemoveTable
#' @importFrom tidyr gather_
#' @details datasource must contain at least the variables description, datasource_type and connect_method.
store_datasource <- function(datasource, conn){
  assert_that(inherits(datasource, "data.frame"))
  assert_that(inherits(conn, "DBIConnection"))

  assert_that(has_name(datasource, "description"))
  assert_that(has_name(datasource, "datasource_type"))
  assert_that(has_name(datasource, "connect_method"))

  hash <- sha1(list(datasource, Sys.time()))
  datasource_type <- store_datasource_type(
    datasource_type = datasource$datasource_type,
    hash = hash,
    conn = conn,
    clean = FALSE
  )
  datasource_parameters <- datasource %>%
    select_(~-description, ~-datasource_type) %>%
    colnames()
  datasource_parameter <- store_datasource_parameter(
    datasource_parameters,
    hash = hash,
    conn = conn,
    clean = FALSE
  )

  datasource %>%
    transmute_(
      id = NA_integer_,
      ~description,
      ~datasource_type
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
      (description, datasource_type)
    SELECT
      d.description,
      dt.id AS datasource_type
    FROM
      (
        staging.%s AS d
      INNER JOIN
        staging.%s AS dt
      ON
        d.datasource_type = dt.description
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
    datasource_type
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
        d.datasource_type = dt.description
      )
    INNER JOIN
      public.datasource AS p
    ON
      p.description = d.description AND
      p.datasource_type = dt.id
    WHERE
      t.description = p.description AND
      t.datasource_type = d.datasource_type;
    ",
    datasource.sql,
    datasource.sql,
    datasource_type
  ) %>%
    dbGetQuery(conn = conn)

  datasource %>%
    gather_(
      key_col = "parameter",
      value_col = "value",
      gather_cols = datasource_parameters,
      na.rm = TRUE
    ) %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("datasource_value_", hash)),
      row.names = FALSE
    )
  datasource_value <- paste0("datasource_value_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sprintf("
    SELECT
      *
    FROM
      public.datasource_value
    WHERE
      public.datasource_value.destroy IS NULL
    ") %>%
    dbGetQuery(conn = conn)

  # destroy values which are no longer used
  sprintf("
    UPDATE
      public.datasource_value AS target
    SET
      destroy = current_timestamp
    FROM
      public.datasource_value AS dvp
    LEFT JOIN
      (
        (
          staging.%s AS dv
        INNER JOIN
          staging.%s AS d
        ON
          dv.description = d.description AND
          dv.datasource_type = d.datasource_type
        )
      INNER JOIN
        staging.%s AS dp
      ON
        dv.parameter = dp.description
      )
    ON
      dvp.datasource = d.id AND
      dvp.parameter = dp.id AND
      dvp.value = dv.value
    WHERE
      dvp.destroy IS NULL AND
      dv.value IS NULL AND
      dvp.datasource = target.datasource AND
      dvp.parameter = target.parameter AND
      dvp.value = target.value AND
      dvp.spawn = target.spawn
    ",
    datasource_value,
    datasource.sql,
    datasource_parameter
  ) %>%
    dbGetQuery(conn = conn)
  # insert new values
  sprintf("
    WITH latest AS
      (
        SELECT
          datasource,
          parameter,
          max(spawn) AS ts
        FROM
          public.datasource_value
        GROUP BY
          datasource, parameter
      )
    INSERT INTO public.datasource_value
      (datasource, parameter, value)
    SELECT
      d.id AS datasource,
      dp.id AS parameter,
      dv.value
    FROM
      (
        (
          staging.%s AS dv
        INNER JOIN
          staging.%s AS d
        ON
          dv.description = d.description AND
          dv.datasource_type = d.datasource_type
        )
      INNER JOIN
        staging.%s AS dp
      ON
        dv.parameter = dp.description
      )
    LEFT JOIN
      (
        latest
      INNER JOIN
        public.datasource_value AS dvp
      ON
        latest.datasource = dvp.datasource AND
        latest.parameter = dvp.parameter AND
        latest.ts = dvp.spawn
      )
    ON
      dvp.datasource = d.id AND
      dvp.parameter = dp.id
    WHERE
      dvp.spawn IS NULL OR
      dvp.destroy IS NOT NULL;",
    datasource_value,
    datasource.sql,
    datasource_parameter
  ) %>%
    dbGetQuery(conn = conn)

  stopifnot(
    dbRemoveTable(conn, c("staging", paste0("datasource_", hash))),
    dbRemoveTable(conn, c("staging", paste0("datasource_value_", hash))),
    dbRemoveTable(conn, c("staging", paste0("datasource_parameter_", hash))),
    dbRemoveTable(conn, c("staging", paste0("datasource_type_", hash)))
  )
  return(hash)
}
