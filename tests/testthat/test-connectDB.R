library(datrProfile)
context("Connect ODBC")

test_that("connectDB.sqlite", {

})


conn.info <- prepareConnection("sqlite", db.name = ":memory:")
conn <- connectDB(conn.info)
str(conn)


odbc::dbWriteTable(conn, "mtcars", mtcars)
odbc::dbListTables(conn)
closeConnection(conn)
profile <- runProfile(conn.info, table = "mtcars")
profile$columnProfile

tempdir()
