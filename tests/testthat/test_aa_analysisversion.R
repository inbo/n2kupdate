context("get_analysis_version()")

test_that("session_package() works", {
  expect_is(
    sp <- session_package(sessionInfo()),
    "data.frame"
  )
  expect_identical(
    colnames(sp),
    c("Description", "Version", "Fingerprint", "Origin", "Revision")
  )
  expect_true(assertthat::has_attr(sp, "AnalysisVersion"))
})

test_that("get_analysis_version() works", {
  expect_is(
    get_analysis_version(sessionInfo()),
    "n2kAnalysisVersion"
  )
})
