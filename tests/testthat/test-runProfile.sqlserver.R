# test sqlserver
c1 <- prepareConnection(db.vendor = "sqlserver",
                        dsn = "SQLDPTPROD_BCBASE_DP", db.encoding = "latin1")
p1 <- runProfile(c1, schema = "bcb", table = "GEO_PAI_PAIS")
p1$columnProfile[[2]]
