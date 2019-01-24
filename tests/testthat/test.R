


p1$columnProfile[[3]]$format.freq

# test teradata
c2 <- prepareConnection(db.vendor = "teradata", db.encoding = "latin1",
                        dsn = "TERADATA_IDQ" )
p2 <- runProfile(c2, schema = "BCBDWDES_DDM", table = "GEOTB_PAI_PAIS")

p2$columnProfile[[2]]$format.freq
library(dplyr)
p2$columnProfile[[1]]$format.freq %>% View

# test sqlite
c3 <- prepareConnection(db.vendor = "sqlite", odbc.driver = RSQLite::SQLite(),
                        db.name = "mydb.sqlite")
p3 <- runProfile(c3, table = "iris", is.parallel = F, query.filter = "Species = 'setosa'")
p3
p4 <- runProfile(c3, table = "mtcars", is.parallel = T)
p4$columnProfile[[1]]

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

