# test teradata
conn <- prepareConnection(db.vendor = "teradata", db.encoding = "latin1",
                        dsn = "TERADATA_IDQ" )
profile <- runProfile(conn, schema = "BCBDWDES_DDM", table = "GEOTB_PAI_PAIS")

profile$columnProfile[[2]]
