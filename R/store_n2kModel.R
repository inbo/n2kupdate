#' extract the results from an n2kModel and stored them
#' @param x the n2kModel object
#' @inheritParams store_datasource_parameter
#' @export
#' @importFrom assertthat assert_that
store_n2kModel <- function(x, conn, hash, clean = TRUE) {
  stopifnot(requireNamespace("n2kanalysis", quietly = TRUE))
  assert_that(inherits(x, "n2kModel"))
  object <- try(n2kanalysis::get_result(x))
  if (!inherits(object, "try-error")) {
    store_n2kResult(object = object, conn = conn, hash = hash, clean = clean)
  }
}
