library(datrProfile)
context("Prepare Connection")

test_that("prepareConnection", {
  x <- list(odbc.driver = odbc::odbc(),
            db.host = "host",
            db.name = NULL,
            dsn = NULL,
            user = "myuser",
            passwd = "mypasswd")
  class(x) <- "teradata"
  expect_equal(prepareConnection(odbc.driver = odbc::odbc(),
                                 db.vendor = "teradata",
                                 db.host = "host",
                                 user = "myuser",
                                 passwd = "mypasswd"), x)
})