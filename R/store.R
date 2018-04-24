#' Store objects of specific classes
#' @param object the object
#' @inheritParams store_datasource_parameter
#' @export
store <- function(object, conn, hash, clean = TRUE){
  UseMethod("store")
}

store.default <- function(object, conn, hash, clean = TRUE) {
  stop(
  "store() has no method for the '",
  paste(class(object), collapse = "', '"),
  "' class",
  call. = FALSE
  )
}
