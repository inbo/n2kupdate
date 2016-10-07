#' Convert all factors in a data.frame to characters
#' @inheritParams base::as.character
#' @export
as.character.data.frame <- function(x, ...){
  factors <- sapply(x, is.factor)
  for (i in which(factors)) {
    x[, i] <- levels(x[, i])[x[, i]]
  }
  return(x)
}
