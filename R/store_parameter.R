#' Store parameters
#' @param parameter a data.frame with parameters. Must contains the variables "description", "local_id", and "parent_parameter_local_id". Other variables are ignored.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr data_frame %>% rowwise mutate_ right_join
#' @importFrom digest sha1
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbBegin dbCommit
store_parameter <- function(parameter, hash, conn, clean = TRUE){
  assert_that(inherits(parameter, "data.frame"))
  assert_that(has_name(parameter, "local_id"))
  assert_that(has_name(parameter, "parent_parameter_local_id"))
  assert_that(has_name(parameter, "description"))
  assert_that(noNA(parameter$description))
  assert_that(noNA(parameter$local_id))
  assert_that(!anyDuplicated(parameter$local_id))
  assert_that(
    all(na.omit(parameter$parent_parameter_local_id) %in% parameter$local_id)
  )
  if (missing(hash)) {
    hash <- sha1(list(parameter, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))

  output <- parameter %>%
    as.character() %>%
    rowwise() %>%
    transmute_(
      ~local_id,
      ~parent_parameter_local_id,
      ~description,
      parent_parameter = ~NA_character_,
      fingerprint = ~ifelse(
        is.na(parent_parameter_local_id),
        sha1(c(description = description, parent_parameter = NA)),
        NA
      ),
      level = 1
    )
  to_do <- sum(is.na(output$fingerprint))
  while (to_do > 0) {
    done <- output %>%
      filter_(~!is.na(fingerprint))
    remainder <- output %>%
      filter_(~is.na(fingerprint)) %>%
      mutate_(level = ~level + 1)
    output <- done %>%
      select_(
        parent_parameter_local_id = ~local_id,
        parent_parameter2 = ~fingerprint
      ) %>%
      right_join(
        remainder,
        by = "parent_parameter_local_id"
      ) %>%
      mutate_(
        parent_parameter = ~ifelse(
          is.na(parent_parameter2),
          parent_parameter,
          parent_parameter2
        ),
        fingerprint = ~ifelse(
          is.na(parent_parameter),
          NA,
          sha1(
            c(description = description, parent_parameter = parent_parameter)
          )
        )
      ) %>%
      select_(~-parent_parameter2) %>%
      bind_rows(done)
    current <- sum(is.na(output$fingerprint))
    if (current == to_do) {
      stop("Error linking parameters to their parent.")
    }
    to_do <- current
  }

  if (clean) {
    dbBegin(conn)
  }
  output %>%
    arrange_(~level, ~fingerprint) %>%
    select_(~description, ~parent_parameter, ~fingerprint, ~level) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("parameter_", hash)),
      row.names = FALSE
    )
  parameter.sql <- paste0("parameter_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  for (level in seq_len(max(output$level))) {
    sprintf("
      WITH cte_parameter AS
      (
        SELECT
          s.fingerprint,
          cp.id AS parent_parameter,
          s.description
        FROM
          staging.%s AS s
        LEFT JOIN
          public.parameter AS cp
        ON
          s.parent_parameter = cp.fingerprint
        WHERE
          s.level = %s
      )

      INSERT INTO public.parameter
        (fingerprint, parent_parameter, description)
      SELECT
        c.fingerprint,
        c.parent_parameter,
        c.description
      FROM
        cte_parameter AS c
      LEFT JOIN
        public.parameter AS p
      ON
        c.fingerprint = p.fingerprint
      WHERE
        p.id IS NULL
      ORDER BY
        c.fingerprint",
      parameter.sql,
      level
    ) %>%
      dbGetQuery(conn = conn)
  }

  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("parameter_", hash)))
    dbCommit(conn)
  }

  output <- output %>%
    select_(~local_id, ~fingerprint)
  attr(output, "hash") <- hash
  attr(output, "SQL") <- parameter.sql
  return(output)
}
