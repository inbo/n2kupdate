conn <- n2khelper::connect_result(
  username = Sys.getenv("N2KRESULT_USERNAME"),
  password = Sys.getenv("N2KRESULT_PASSWORD")
)$con
