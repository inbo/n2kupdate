#' The n2kAnalysisVersion class
#' @name n2kAnalysisVersion-class
#' @rdname n2kAnalysisVersion-class
#' @exportClass n2kAnalysisVersion
#' @aliases n2kAnalysisVersion-class
#' @importFrom methods setClass
#' @docType class
setClass(
  "n2kAnalysisVersion",
  representation = representation(
    AnalysisVersion = "data.frame",
    RPackage = "data.frame",
    AnalysisVersionRPackage = "data.frame"
  ),
  prototype = prototype(
    AnalysisVersion = data.frame(
      Fingerprint = character(0),
      stringsAsFactors = FALSE
    ),
    RPackage = data.frame(
      Fingerprint = character(0),
      Description = character(0),
      Version = character(0),
      stringsAsFactors = FALSE
    ),
    AnalysisVersionRPackage = data.frame(
      AnalysisVersion = character(0),
      RPackage = character(0),
      stringsAsFactors = FALSE
    )
  )
)

#' @importFrom methods setValidity
#' @importFrom assertthat assert_that has_name
setValidity(
  "n2kAnalysisVersion",
  function(object){
    assert_that(has_name(object@AnalysisVersion, "Fingerprint"))

    assert_that(has_name(object@RPackage, "Fingerprint"))
    assert_that(has_name(object@RPackage, "Description"))
    assert_that(has_name(object@RPackage, "Version"))

    assert_that(has_name(object@AnalysisVersionRPackage, "AnalysisVersion"))
    assert_that(has_name(object@AnalysisVersionRPackage, "RPackage"))

    if (!all(
      object@AnalysisVersionRPackage$AnalysisVersion %in%
        object@AnalysisVersion$Fingerprint
    )) {
      stop(
"Some AnalysisVersion in 'AnalysisVersionRPackage' slot are not present in
'AnalysisVersion' slot"
      )
    }
    if (!all(
      object@AnalysisVersionRPackage$RPackage %in% object@RPackage$Fingerprint
    )) {
      stop(
"Some AnalysisVersion in 'AnalysisVersionRPackage' slot are not present in
'AnalysisVersion' slot"
      )
    }
    if (anyDuplicated(object@AnalysisVersionRPackage)) {
      stop("Duplicated rows in 'AnalysisVersionRPackage' slot")
    }
    if (anyDuplicated(object@AnalysisVersion)) {
      stop("Duplicated rows in 'AnalysisVersion' slot")
    }
    if (anyDuplicated(object@RPackage)) {
      stop("Duplicated rows in 'RPackage' slot")
    }
    return(TRUE)
  }
)
