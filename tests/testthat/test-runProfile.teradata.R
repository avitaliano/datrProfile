library(datrProfile)
context("Run Profile Teradata")

test_that("runProfile.teradata", {
  conn.info <- prepareConnection(db.vendor = "teradata",
                            db.encoding = "latin1",
                            dsn = "TERADATA_DSN" )

  # conn <- connectDB(conn.info)
  # odbc::dbSendQuery(conn, "database MY_SCHEMA")
  # odbc::dbWriteTable(conn, "MTCARS", mtcars, schema = "MY_SCHEMA", 
  #                    overwrite = TRUE, temporary = FALSE, row.names = FALSE)
  # closeConnection(conn)
  # profile = runProfile(conn.info, table = "MTCARS", schema = "MY_SCHEMA")
  # expect_equal(profile$columnProfile[[1]]$min.value, min(mtcars$mpg))
  # expect_equal(profile$columnProfile[[2]]$max.value, max(mtcars$cyl))
  # expect_equal(profile$columnProfile[[3]]$count.distinct, length(unique(mtcars$disp)))
})


