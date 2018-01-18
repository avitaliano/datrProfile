
c2 <- prepareConnection(db.vendor = "teradata",
                        dsn = "TERADATA_IDQ" )
p2 <- runProfile(c2, schema = "BCBDWDES_DDM", table = "GEOTB_MUN_MUNICIPIO")


c1 <- prepareConnection(db.vendor = "sqlserver",
                        dsn = "SQL_BCBASE_DP", db.encoding = "latin1")
p1 <- runProfile(c1, schema = "bcb", table = "GEO_MUN_MUNICIPIO")

p1
p2


# format profile
str(c)
c$format <- f(c$value)
str(c)

View(c %>% group_by(format) %>% summarize(format.freq = sum(freq)))

x <- c[[1]]
f <- function(x){
  x <- stringr::str_replace_all(x, "[0-9]", "9")
  x <- stringr::str_replace_all(x, "[:alpha:]", "X")
  x <- stringr::str_replace_all(x, "[:blank:]", "b")
  x <- stringr::str_replace_all(x, "[:punct:]", "S")
 return(x)
}

library(RSQLite)
#con <- odbc::dbConnect(RSQLite::SQLite(), ":memory:")
con <- odbc::dbConnect(RSQLite::SQLite(), "mydb.sqlite")
odbc::dbListTables(con)
odbc::dbWriteTable(con, "mtcars", mtcars)
odbc::dbWriteTable(con, "iris", iris)
odbc::dbGetQuery(con, "SELECT hp as value, count(*) as freq from mtcars group by hp order by freq desc limit 4")
odbc::dbGetQuery(con, "select *  from pragma_table_info('iris')")
odbc::dbGetQuery(con, "SELECT '' as table_schema, 'iris' as table_name, name as column_name, type as column_datatype, 0 AS column_length, 0 AS column_precision FROM pragma_table_info('iris')")

p <- runProfile(con, table = )


