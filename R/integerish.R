is.integerish <- function(x) {
  is.integer(x) || (is.numeric(x) && all(x == as.integer(x)))
}
