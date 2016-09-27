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

    package_data_frame <- function(package){
      if ("Repository" %in% names(package)) {
        return(
          data.frame(
            Description = package$Package,
            Version = package$Version,
            Origin = package$Repository,
            Revision = NA_character_,
            stringsAsFactors = FALSE
          )
        )
      }
      if ("GithubRepo" %in% names(package)) {
        return(
          data.frame(
            Description = package$Package,
            Version = package$Version,
            Origin = paste(
              "https://github.com/",
              package$GithubUsername,
              package$GithubRepo,
              sep = "/"
            ),
            Revision = package$GithubSHA1,
            stringsAsFactors = FALSE
          )
        )
      }
      data.frame(
        Description = package$Package,
        Version = package$Version,
        Origin = NA_character_,
        Revision = NA_character_,
        stringsAsFactors = FALSE
      )
    }

    package <- data.frame(
      Description = c(session$running, "R"),
      Version = c(
        session$R.version$platform,
        paste(
          session$R.version[c("major", "minor")],
          collapse = "."
        )
      ),
      Origin = NA_character_,
      Revision = c(NA_character_, session$R.version$`svn rev`),
      stringsAsFactors = FALSE
    )
    if ("otherPkgs" %in% names(session)) {
      package <- lapply(session$otherPkgs, package_data_frame) %>%
        bind_rows(package)
    }
    if ("loadedOnly" %in% names(session)) {
      package <- lapply(session$loadedOnly, package_data_frame) %>%
        bind_rows(package)
    }
    rownames(package) <- NULL
    package <- package %>%
      rowwise() %>%
      mutate_(
        Fingerprint = ~sha1(list(
          Description = Description,
          Version = Version,
          Origin = Origin,
          Revision = Revision
        ))
      ) %>%
      arrange_(~Fingerprint) %>%
      as.data.frame()
    attr(package, "AnalysisVersion") <- sha1(package)
    return(package)
  }
)
