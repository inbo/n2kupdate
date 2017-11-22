context("get_analysis_version()")

test_that("session_package() works", {
  expect_is(
    sp <- session_package(sessionInfo()),
    "data.frame"
  )
  expect_identical(
    colnames(sp),
    c("Description", "Version", "Origin", "Fingerprint")
  )
  expect_true(assertthat::has_attr(sp, "AnalysisVersion"))
})

test_that("get_analysis_version() works", {
  expect_is(
    get_analysis_version(sessionInfo()),
    "n2kAnalysisVersion"
  )
})

test_that("sha1.n2kAnalysisVersion() works", {
  ut.analysis_version <- get_analysis_version(sessionInfo())
  expect_is(
    hash <- sha1(ut.analysis_version),
    "character"
  )
  expect_identical(nchar(hash), 40L)

  ut.analysis_version@AnalysisVersion <- ut.analysis_version@AnalysisVersion %>%
    mutate_(Fingerprint = ~factor(Fingerprint))
  ut.analysis_version@RPackage <- ut.analysis_version@RPackage %>%
    mutate_(Fingerprint = ~factor(Fingerprint))
  ut.analysis_version@AnalysisVersionRPackage <-
    ut.analysis_version@AnalysisVersionRPackage %>%
      mutate_(AnalysisVersion = ~factor(AnalysisVersion))
  expect_is(
    hash2 <- sha1(ut.analysis_version),
    "character"
  )
  expect_identical(nchar(hash2), 40L)
  expect_identical(hash, hash2)

})
