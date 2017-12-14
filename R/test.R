c1 <- prepareConnection(db.vendor = "sqlserver",
                        dsn = "SQL_BCBASE_DP")
c2 <- prepareConnection(db.vendor = "teradata",
                        dsn = "TERADATA_IDQ" )
p1 <- runProfile(c1, schema = "bcb", table = "GEO_CON_CONTINENTE")
p2 <- runProfile(c2, schema = "BCBDWDES_DDM", table = "GEOTB_PAI_PAIS")

View(p1$columnProfile)
View(p2$columnProfile)

p1$columnProfile[[5]]
