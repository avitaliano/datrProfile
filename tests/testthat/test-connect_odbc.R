library(datrProfile)
context("Connect ODBC")

test_that("prepare_connection", {
  x <- list(db.host = "192.168.0.36",
            db.name = NULL,
            dsn = NULL,
            user = "myuser",
            passwd = "mypasswd")
  class(x) <- "teradata"
  expect_equal(prepare_connection(db.vendor = "teradata",
                                  db.host = "192.168.0.36",
                                  user = "myuser",
                                  passwd = "mypasswd"), x)
})
