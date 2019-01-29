library(datrProfile)
context("Run Profile SqlServer")

test_that("runProfile.sqlserver", {
  conn.sqlserver <- prepareConnection(db.vendor = "sqlserver",
                            dsn = "SQLDPTPROD_BCBASE_DP",
                            db.encoding = "latin1")
  #  profile.sqlserver <- runProfile(conn.sqlserver,
  #                        schema = "bcb",
  #                        table = "GEO_MUN_MUNICIPIO",
  #                        is.parallel = FALSE,
  #                        format.show.percentage = 0.03)
  #
  #
  # profile.sqlserver$columnProfile[[4]]
  # expect_equal("bcb", profile.sqlserver[[1]])
  # expect_equal("PES_TPE_TIPO_PESSOA", profile.sqlserver[[2]])
  # expect_equal(profile.sqlserver[[3]][[1]]$count.distinct, 6)
  # expect_equal(profile.sqlserver[[3]][[1]]$perc.distinct, 1)
  # expect_equal(profile.sqlserver[[3]][[1]]$count.null, 0)
  # expect_equal(profile.sqlserver[[3]][[1]]$min.value, 1)
  # expect_equal(profile.sqlserver[[3]][[1]]$max.value, 6)
})
