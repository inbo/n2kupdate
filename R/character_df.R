#' Convert all factors in a data.frame to characters
#' @inheritParams base::as.character
#' @export
#' @importFrom assertthat assert_that
character_df <- function(x, ...){
  assert_that(
    inherits(x, "data.frame"),
    msg = paste(
      deparse(substitute(x)),
      "does not inherit from class data.frame"
    )
  )
  factors <- sapply(x, is.factor)
  for (i in which(factors)) {
    x[[i]] <- levels(x[[i]])[x[[i]]]
  }
  return(x)
}
