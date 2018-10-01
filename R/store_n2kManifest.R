#' store all models from an n2kManifest
#' @param manifest a \code{\link[n2kanalysis]{n2kManifest-class}}
#' @param status the status of the objects to be imported
#' @inheritParams store_datasource_parameter
#' @inheritParams n2kanalysis::read_model
#' @export
#' @importFrom assertthat assert_that is.string
#' @importFrom dplyr %>% mutate left_join transmute distinct arrange
#' @importFrom rlang .data
store_n2kManifest <- function(
  manifest, base, project, conn, status = "converged", hash, clean = TRUE
) {
  assert_that(inherits(manifest, "n2kManifest"))

  m <- manifest@Manifest %>%
    mutate(Level = ifelse(is.na(.data$Parent), 0, 1))
  while (!all(is.na(m$Parent))) {
    m <- m %>%
      left_join(manifest@Manifest, by = c("Parent" = "Fingerprint")) %>%
      transmute(
        .data$Fingerprint,
        Parent = .data$Parent.y,
        Level = ifelse(is.na(.data$Parent), .data$Level, .data$Level + 1)
      )
  }
  m %>%
    distinct(.data$Fingerprint, .data$Level) %>%
    arrange(.data$Level, .data$Fingerprint) -> m

  if (inherits(base, "s3_bucket")) {
    stopifnot(requireNamespace("aws.s3", quietly = TRUE))
    assert_that(is.string(project))
    assert_that(is.character(status))
    assert_that(length(status) > 0)

    available <- aws.s3::get_bucket(bucket = base, prefix = project, max = Inf)
    keys <- sapply(available, `[[`, "Key")
    re <- sprintf(
      "%s/.*/(%s).rds",
      project,
      paste(m$Fingerprint, collapse = "|")
    )
    relevant <- grep(re, keys)
    re <- sprintf(
      "%s/(%s)/[[:xdigit:]]{40}.rds",
      project,
      paste(status, collapse = "|")
    )
    if (!all(grepl(re, keys[relevant]))) {
      return(NULL)
    }
    message("downloading objects from S3 bucket")
    stopifnot(requireNamespace("n2kanalysis", quietly = TRUE))
    for (x in sort(keys[relevant])) {
      message(x)
      x <- try(n2kanalysis::read_model(basename(x), base = base, project = project))
      if (inherits(x, "try-error")) {
        warning("Failed to read ", x)
      } else {
        store_n2kModel(x, conn = conn, hash = hash, clean = clean)
      }
      return(invisible(NULL))
    }
  }
  assert_that(is.string(base))
  assert_that(dir.exists(base))
  message("reading objects from local file")
  stopifnot(requireNamespace("n2kanalysis", quietly = TRUE))
  for (x in m$Fingerprint) {
    message(x)
    x <- try(n2kanalysis::read_model(x, base = base, project = project))
    if (inherits(x, "try-error")) {
      warning("Failed to read ", x)
    } else {
      store_n2kModel(x, conn = conn, hash = hash, clean = clean)
    }
  }
  return(invisible(NULL))
}
