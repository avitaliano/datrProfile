library(datrProfile)
context("Prepare Connection")

test_that("prepareConnection", {
  conn.info <- list(odbc.driver = odbc::odbc(),
                    db.host = "host",
                    db.name = NULL,
                    db.encoding = "",
                    dsn = NULL,
                    user = "myuser",
                    passwd = "mypasswd")
  class(conn.info) <- "teradata"
  expect_equal(prepareConnection(db.vendor = "teradata",
                                 db.host = "host",
                                 user = "myuser",
                                 passwd = "mypasswd"), conn.info)
})

