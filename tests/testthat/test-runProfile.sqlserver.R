library(datrProfile)
context("Run Profile SqlServer")

test_that("runProfile.sqlserver", {
  conn.info <- prepareConnection(db.vendor = "sqlserver",
                            dsn = "SQLSERVER_DSN",
                            db.encoding = "latin1")

  # conn <- DBI::dbConnect(odbc::odbc(),
  #                        Driver   = "Sql Server",
  #                        Server   = "HOST",
  #                        Database = "MY_DB",
  #                        Port     = 1433)
  # 
  # table <- DBI::Id(catalog = "MY_DB", schema = "dbo", table = "mtcars")
  # unquoted <- DBI::dbUnquoteIdentifier(DBI::ANSI(), table)
  # quoted <- DBI::dbQuoteIdentifier(DBI::ANSI(), table)
  # unquoted
  # quoted
  # 
  # DBI::dbWriteTable(conn, name = table, mtcars)
  # DBI::dbWriteTable(conn, name = unquoted, mtcars)
  # DBI::dbWriteTable(conn, name = quoted, mtcars)
  # odbc::dbWriteTable(conn, name = "MTCARS", mtcars)
  # conn <- closeConnection(conn)
  # profile = runProfile(conn.info, table = "MTCARS",
  #                      schema=schema,
  #                      is.parallel = FALSE)
  # expect_equal(profile$columnProfile[[1]]$min.value, min(mtcars$mpg))
  # expect_equal(profile$columnProfile[[2]]$max.value, max(mtcars$cyl))
  # expect_equal(profile$columnProfile[[3]]$count.distinct, length(unique(mtcars$disp)))
})

