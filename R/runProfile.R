# runProfile

#' Profiles datasets (collecting statistics and informative summaries
#' about that data) on data frames and ODBC tables: max, min, avg, sd, nulls,
#' distinct values, data patterns, data/format frequencies.
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table schema
#' @param table Table name
#' @param is.parallel Boolean that indicates if profile will run in parallel
#' @param count.nodes Number of nodes used when is.parallel = TRUE
#' @param query.filter Filter applied to the table, when profilling
#'
#' @return profile results for the table
#' @export
runProfile <- function(conn.info, schema = NULL, table,
                       is.parallel = TRUE,
                       count.nodes = 5,
                       query.filter = NA){

  print(paste0(Sys.time()," Started profile at table ", schema, ".", table))

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
                                               conn.info = conn.info,
                                               schema = schema,
                                               table = table,
                                               column = x,
                                               getColumnDatatype(x,
                                                                 columns.metadata),
                                             query.filter = query.filter))

  snow::stopCluster(cluster)
  } else{
    # call profileColumn for each table's column
    profile$columnProfile <- lapply(columns.metadata$column_name,
                                    function(x) profileColumn(
                                      conn.info = conn.info,
                                      schema = schema,
                                      table = table,
                                      column = x,
                                      column.datatype = getColumnDatatype(x,
                                                                          columns.metadata),
                                      query.filter = query.filter))
  }

  profile$endtime = Sys.time()
  print(paste0(profile$endtime," Ended profile at table ", schema, ".", table))
  return(profile)
}
