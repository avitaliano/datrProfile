library(datrProfile)
context("Profile Column Teradata")

test_that("profileColumn.teradata", {
  schema <- "BCBDWDES_DDM"
  table <- "GEOTB_CON_CONTINENTE"
  conn.info <- prepareConnection(db.vendor = "teradata",
                                 db.encoding = "latin1",
                                 dsn = "TERADATA_IDQ" )
})
