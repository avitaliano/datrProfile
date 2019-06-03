library(datrProfile)
context("Run Profile SQLite")

test_that("runProfile.sqlite", {

  #http://rstat.consulting/blog/temporary-dir-and-files-in-r/
  # creates temp sqlite db
  sqlite = tempfile(fileext = ".sqlite")

  conn.info = prepareConnection(db.vendor = "sqlite", db.name = sqlite)
  conn = connectDB(conn.info)
  odbc::dbWriteTable(conn, "mtcars", mtcars)
  closeConnection(conn)
  profile = runProfile(conn.info, table = "mtcars")
  profile$columnProfile[[1]]$min.value

  max(mtcars$mpg)

  expect_equal(profile$columnProfile[[1]]$min.value, min(mtcars$mpg))
  expect_equal(profile$columnProfile[[1]]$max.value, max(mtcars$mpg))
})