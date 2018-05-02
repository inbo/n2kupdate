#' Store the analysis version in the database
#' @param analysis_version an n2kAnalysisVersion object. See
#'   \code{\link{get_analysis_version}}
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that noNA is.flag is.string
#' @importFrom digest sha1
#' @importFrom dplyr %>% rowwise mutate_ select_ arrange_ inner_join
#' @importFrom DBI dbWriteTable dbQuoteIdentifier dbGetQuery dbRemoveTable dbBegin dbCommit
store_analysis_version <- function(analysis_version, hash, clean = TRUE, conn){
  assert_that(inherits(analysis_version, "n2kAnalysisVersion"))
  assert_that(inherits(conn, "DBIConnection"))
  assert_that(is.flag(clean))
  assert_that(noNA(clean))
  if (missing(hash)) {
    hash <- sha1(list(analysis_version, as.POSIXct(Sys.time())))
  } else {
    assert_that(is.string(hash))
  }

  av <- analysis_version@AnalysisVersion
  rp <- analysis_version@RPackage
  avrp <- analysis_version@AnalysisVersionRPackage

  av <- character_df(av)
  rp <- character_df(rp)
  avrp <- character_df(avrp)

  if (clean) {
    dbBegin(conn)
  }

  av %>%
    select_(fingerprint = ~Fingerprint) %>%
    arrange_(~fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("analysis_version_", hash)),
      row.names = FALSE
    )
  rp %>%
    select_(
      fingerprint = ~Fingerprint,
      description = ~Description,
      version = ~Version,
      origin = ~Origin
    ) %>%
    arrange_(~fingerprint) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("r_package_", hash)),
      row.names = FALSE
    )
  avrp %>%
    select_(analysis_version = ~AnalysisVersion, r_package = ~RPackage) %>%
    arrange_(~analysis_version, ~r_package) %>%
    as.data.frame() %>%
    dbWriteTable(
      conn = conn,
      name = c("staging", paste0("avrp_", hash)),
      row.names = FALSE
    )
  sql.av <- paste0("analysis_version_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sql.rp <- paste0("r_package_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  sql.avrp <- paste0("avrp_", hash) %>%
    dbQuoteIdentifier(conn = conn)
  # write new analysis version
  sprintf("
    INSERT INTO public.analysis_version
      (fingerprint)
    SELECT
      s.fingerprint
    FROM
      staging.%s AS s
    LEFT JOIN
      public.analysis_version AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    sql.av
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    INSERT INTO public.r_package
      (fingerprint, description, version, origin)
    SELECT
      s.fingerprint,
      s.description,
      s.version,
      s.origin
    FROM
      staging.%s AS s
    LEFT JOIN
      public.r_package AS p
    ON
      s.fingerprint = p.fingerprint
    WHERE
      p.id IS NULL",
    sql.rp
  ) %>%
    dbGetQuery(conn = conn)
  sprintf("
    INSERT INTO public.analysis_version_r_package
      (analysis_version, r_package)
    SELECT
      pav.id AS analysis_version,
      prp.id AS r_package
    FROM
      (
        (
          public.analysis_version AS pav
        INNER JOIN
          staging.%s AS s
        ON
          pav.fingerprint = s.analysis_version
        )
      INNER JOIN
        public.r_package AS prp
      ON
        prp.fingerprint = s.r_package
      )
    LEFT JOIN
      public.analysis_version_r_package AS pavrp
    ON
      pav.id = pavrp.analysis_version AND
      prp.id = pavrp.r_package
    WHERE
      pavrp.analysis_version IS NULL",
    sql.avrp
  ) %>%
    dbGetQuery(conn = conn)
  if (clean) {
    dbRemoveTable(conn, c("staging", paste0("analysis_version_", hash)))
    dbRemoveTable(conn, c("staging", paste0("r_package_", hash)))
    dbRemoveTable(conn, c("staging", paste0("avrp_", hash)))
    dbCommit(conn)
  }

  return(hash)
}
