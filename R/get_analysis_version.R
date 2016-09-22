#' Get an analysis version
#' @param version the object to extract the version
#' @name get_analysis_version
#' @rdname get_analysis_version
#' @exportMethod get_analysis_version
#' @docType methods
#' @importFrom methods setGeneric
setGeneric(
  name = "get_analysis_version",
  def = function(version){
    standardGeneric("get_analysis_version")
  }
)

#' @rdname get_analysis_version
#' @aliases get_analysis_version,sessionInfo-methods
#' @importFrom methods setMethod new
#' @importFrom assertthat assert_that has_name has_attr
#' @importFrom dplyr %>% arrange_
#' @include class_n2kAnalysisVersion.R
setMethod(
  f = "get_analysis_version",
  signature = signature(version = "data.frame"),
  definition = function(version){
    assert_that(has_name(version, "Description"))
    assert_that(has_name(version, "Version"))
    assert_that(has_name(version, "Fingerprint"))
    assert_that(has_attr(version, "AnalysisVersion"))

    analysis.version <- data.frame(
      Fingerprint = attr(version, "AnalysisVersion"),
      stringsAsFactors = FALSE
    )
    version <- version %>%
      arrange_(~Fingerprint)
    new(
      "n2kAnalysisVersion",
      AnalysisVersion = analysis.version,
      RPackage = version,
      AnalysisVersionRPackage = data.frame(
        AnalysisVersion = analysis.version$Fingerprint,
        RPackage = version$Fingerprint,
        stringsAsFactors = FALSE
      )
    )
  }
)

#' @rdname get_analysis_version
#' @aliases get_analysis_version,sessionInfo-methods
#' @importFrom methods setMethod new
#' @importFrom utils sessionInfo
#' @include import_S3_classes.R
setMethod(
  f = "get_analysis_version",
  signature = signature(version = "sessionInfo"),
  definition = function(version){
    get_analysis_version(version = session_package(version))
  }
)
