# runProfile

#' Runs a profile at the table at the database over all table's columns.
#'
#' @param conn.info Connection info created at \code{\link{prepareConnection()}}
#' @param schema Table schema
#' @param table Table name
#' @param is.parallel Boolean that indicates if profile will run in parallel
#' @param count.nodes Number of nodes used when is.parallel = TRUE
#'
#' @return profile results for the table
#' @export
#'
#' @examples
#' conn.info <- prepareConnection(db.vendor = "teradata",
#'    dsn = "ODBC_MYDB", user = "myuser", passwd = "mypasswd")
#' p <- runProfile(conn.info, "table_schema", "my_table")
runProfile <- function(conn.info, schema = NULL, table,
                       is.parallel = TRUE,
                       count.nodes = 5){

  print(paste0("Starting profile at table ", schema, ".", table,
               " at ", Sys.time()))

  profile <- list( schema = schema,
                   table = table,
                   columnProfile = NULL,
                   starttime = Sys.time(),
                   endtime = NULL)
  class(profile) <- "profile"

  # Starting Column Profile
  columns.metadata <- getTableColumns(conn.info, schema, table)

  # TODO: issue, parallel not working with sqlite. temporary disabled.
  if ( is.parallel && class(conn.info) != "sqlite" ){
    # initializes cluster
    cluster <- snow::makeSOCKcluster(count.nodes)

    # initializes loging
    snow::clusterApply(cluster, seq_along(cluster), function(i) {
      if (!dir.exists("log"))
        dir.create("log")
      zz <- file(file.path("log", sprintf("parallel-runProfile-%d.Rout", i)),
                 open = "wt")
      sink(zz)
      sink(zz, type = "message")
    })

    # export local functions used on runProfile call
    local.functions <- list ("runProfile",
                             "profileColumn",
                             "getTableColumns",
                             "connectDB",
                             "closeConnection",
                             "buildQueryColumnMetadata",
                             "buildQueryCountTotal",
                             "buildQueryCountNull",
                             "buildQueryColumnStats",
                             "buildQueryColumnFrequency")
                             # TODO: observe if it's not necessary S3 exports,
                             # "buildQueryColumnMetadata.sqlserver",
                             # "buildQueryCountTotal.sqlserver",
                             # "buildQueryCountNull.sqlserver",
                             # "buildQueryColumnStats.sqlserver",
                             # "buildQueryColumnFrequency.sqlserver"
                             # )
    snow::clusterExport(cluster, local.functions)

    # call profileColumn for each table's column
    profile$columnProfile <- snow::parLapply(cluster,
                                             columns.metadata$column_name,
                                             function(x) profileColumn(
                                               conn.info,
                                               column = x,
                                               table,
                                               schema))

  snow::stopCluster(cluster)
  } else{
    # call profileColumn for each table's column
    profile$columnProfile <- lapply(columns.metadata$column_name,
                                    function(x) profileColumn(conn.info,
                                                              column = x,
                                                              table,
                                                              schema))
  }

  profile$endtime = Sys.time()
  return(profile)
}
