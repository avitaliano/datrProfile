library(datrProfile)
context("runProfile.teradata")

test_that("runProfile.teradata", {
  conn <- prepareConnection(db.vendor = "teradata",
                            db.encoding = "latin1",
                            dsn = "TERADATA_IDQ" )
  profile <- runProfile(conn,
                        schema = "BCBDWDES_DDM",
                        table = "PESTB_TPE_TIPO_PESSOA",
                        is.parallel = FALSE)
  expect_equal("BCBDWDES_DDM", profile[[1]])
  expect_equal("PESTB_TPE_TIPO_PESSOA", profile[[2]])
  expect_equal(profile[[3]][[1]]$count.distinct, 6)
  expect_equal(profile[[3]][[1]]$perc.distinct, 1)
  expect_equal(profile[[3]][[1]]$count.null, 0)
  expect_equal(profile[[3]][[1]]$min.value, 1)
  expect_equal(profile[[3]][[1]]$max.value, 6)
})



