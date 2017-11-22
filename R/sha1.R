#' @importFrom digest sha1
#' @export sha1
digest::sha1

#' @importFrom digest sha1
#' @importFrom dplyr %>% mutate_each_
#' @export
#' @method sha1 n2kAnalysisVersion
sha1.n2kAnalysisVersion <- function(x, digits = 14L, zapsmall = 7L, ...) {
  av <- x@AnalysisVersion
  rp <- x@RPackage
  avrp <- x@AnalysisVersionRPackage

  av <- character_df(av)
  rp <- character_df(rp)
  avrp <- character_df(avrp)

  z <- list(
    analysis_version = av %>%
      arrange_(~Fingerprint),
    r_package = rp %>%
      arrange_(~Fingerprint),
    analysis_version_r_package = avrp %>%
      arrange_(~AnalysisVersion, ~RPackage)
  )
  attr(z, "digest::sha1") <- list(
    class = class(x),
    digits = as.integer(digits),
    zapsmall = as.integer(zapsmall),
    ...
  )
  sha1(z, digits = digits, zapsmall = zapsmall, ...)
}
