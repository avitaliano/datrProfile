library(datrProfile)
context("Run Profile Teradata")

test_that("runProfile.teradata", {
  conn.info <- prepareConnection(db.vendor = "teradata",
                            db.encoding = "latin1",
                            dsn = "TERADATA_IDQ" )

  conn <- connectDB(conn.info)


  odbc::dbWriteTable(conn, "BCBDWDES_STG.MTCARS", mtcars)
})



