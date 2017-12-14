
c2 <- prepareConnection(db.vendor = "teradata",
                        dsn = "TERADATA_IDQ" )
p2 <- runProfile(c2, schema = "BCBDWDES_DDM", table = "GEOTB_PAI_PAIS")


c1 <- prepareConnection(db.vendor = "sqlserver",
                        dsn = "SQL_BCBASE_DP")
p4 <- runProfile(c1, schema = "bcb", table = "GEO_CON_CONTINENTE")

p4$columnProfile[[1]]

system.time(p.seq <- runProfile(c1, schema = "bcb",
                                table = "GEO_MUN_MUNICIPIO",
                                is.parallel = TRUE)
)

devtools::clea