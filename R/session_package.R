#' Convert a sessionInfo() to a data.frame of packages
#' @param session The output of sessionInfo()
#' @return a data.frame with the packages of a sessionInfo()
#' @name session_package
#' @rdname session_package
#' @exportMethod session_package
#' @docType methods
#' @importFrom methods setGeneric
setGeneric(
  name = "session_package",
  def = function(session){
    standardGeneric("session_package") # nocov
  }
)

#' @rdname session_package
#' @aliases session_package,sessionInfo-methods
#' @importFrom methods setMethod new
#' @importFrom digest sha1
#' @importFrom utils sessionInfo
#' @importFrom dplyr %>% bind_rows rowwise arrange_
setMethod(
  f = "session_package",
  signature = signature(session = "sessionInfo"),
  definition = function(session){
    package <- data.frame(
      Description = c(session$running, "R"),
      Version = c(
        session$R.version$platform,
        paste(
          session$R.version[c("major", "minor")],
          collapse = "."
        )
      ),
      stringsAsFactors = FALSE
    )
    if ("otherPkgs" %in% names(session)) {
      package <- lapply(
        session$otherPkgs,
        function(i){
          data.frame(
            Description = i$Package,
            Version = i$Version,
            stringsAsFactors = FALSE
          )
        }
      ) %>%
        bind_rows(package)
    }
    if ("loadedOnly" %in% names(session)) {
      package <- lapply(
        session$loadedOnly,
        function(i){
          data.frame(
            Description = i$Package,
            Version = i$Version,
            stringsAsFactors = FALSE
          )
        }
      ) %>%
        bind_rows(package)
    }
    rownames(package) <- NULL
    package <- package %>%
      rowwise() %>%
      mutate_(
        Fingerprint = ~sha1(c(Description = Description, Version = Version))
      ) %>%
      arrange_(~Fingerprint) %>%
      as.data.frame()
    attr(package, "AnalysisVersion") <- sha1(package)
    return(package)
  }
)
