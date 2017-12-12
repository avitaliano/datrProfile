# runProfile

conn.info <- prepareConnection(db.vendor = "sqlserver",
                               dsn = "SQL_DEINF_QUALIDADOS_D")
schema <- "dbo"
table <- "PAI_PAIS_SQL"
database <- "DEINF_QUALIDADOS_D"

runProfile <- function(conn.info, schema, table, is.parallel = FALSE){

  print(paste0("Starting profile at table ", schema, ".", table,
               " at ", Sys.time()))

  # load queries used to profile table
  loadQueries(conn.info)

  profile <- list( schema = schema,
                   table = table,
                   columnProfile = NULL,
                   start_exec_time = Sys.time(),
                   end_exec_time = NULL)

  # Starting Column Profile
  columns.metadata <- getTableColumns(conn.info, schema, table)
  columns.metadata
}

runProfile(conn.info, schema, table)
