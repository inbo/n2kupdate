#' Store parameters
#' @param parameter a data.frame with parameters. Must contains the variables "description", "local_id", and "parent_parameter_local_id". Other variables are ignored.
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.string is.flag
#' @importFrom methods is
#' @importFrom digest sha1
#' @importFrom dplyr %>% transmute filter mutate select right_join bind_rows arrange
#' @importFrom rlang .data
#' @importFrom purrr map2_chr
#' @importFrom digest sha1
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbBegin dbCommit
#' @importFrom stats na.omit
store_parameter <- function(parameter, hash, conn, clean = TRUE){
  parameter <- character_df(parameter)
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

  parameter %>%
    transmute(
      .data$local_id,
      .data$parent_parameter_local_id,
      .data$description,
      parent_parameter = NA_character_,
      fingerprint = map2_chr(
        .data$description,
        .data$parent_parameter_local_id,
        ~ifelse(
          is.na(.y),
          sha1(c(description = .x, parent_parameter = NA)),
          NA
        )
      ),
      level = 1
    ) -> output
  to_do <- sum(is.na(output$fingerprint))
  while (to_do > 0) {
    output %>%
      filter(!is.na(.data$fingerprint)) -> done
    output %>%
      filter(is.na(.data$fingerprint)) %>%
      mutate(level = .data$level + 1) -> remainder
    done %>%
      select(
        parent_parameter_local_id = .data$local_id,
        parent_parameter2 = .data$fingerprint
      ) %>%
      right_join(
        remainder,
        by = "parent_parameter_local_id"
      ) %>%
      mutate(
        parent_parameter = ifelse(
          is.na(.data$parent_parameter2),
          .data$parent_parameter,
          .data$parent_parameter2
        ),
        fingerprint = map2_chr(
          .data$description,
          .data$parent_parameter,
          ~ifelse(
            is.na(.y),
            NA,
            sha1(c(description = .x, parent_parameter = .y))
          )
        )
      ) %>%
      select(-.data$parent_parameter2) %>%
      bind_rows(done) -> output
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
    arrange(.data$level, .data$fingerprint) %>%
    select(
      .data$description,
      .data$parent_parameter,
      .data$fingerprint,
      .data$level
    ) %>%
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

  output %>%
    select(.data$local_id, .data$fingerprint) -> output
  attr(output, "hash") <- hash
  attr(output, "SQL") <- parameter.sql
  return(output)
}
