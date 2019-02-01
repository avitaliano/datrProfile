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
#' @param format.show.percentage Threshold considered when showing formats'
#' percentages
#'
#' @return profile results for the table
#' @export
runProfile <- function(conn.info, schema = NULL, table,
                       is.parallel = TRUE,
                       count.nodes,
                       query.filter = NA,
                       format.show.percentage = 0.03){

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
    if (missing(count.nodes)) {
      #cluster <- parallel::makeSOCKcluster(parallel::detectCores())
      cluster <- parallel::makePSOCKcluster(parallel::detectCores())
    } else {
      cluster <- parallel::makePSOCKcluster(count.nodes)
    }

    # initializes loging
    parallel::clusterApply(cluster, seq_along(cluster), function(i) {
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

    parallel::clusterExport(cluster, local.functions)

    # call profileColumn for each table's column
    profile$columnProfile <- parallel::parLapply(cluster,
                                             columns.metadata$column_name,
                                             function(x) profileColumn(
                                               conn.info = conn.info,
                                               schema = schema,
                                               table = table,
                                               column = x,
                                               column.datatype =
                                                 getColumnDatatype(x,
                                                                   columns.metadata),
                                             query.filter = query.filter,
                                             format.show.percentage = format.show.percentage))

  parallel::stopCluster(cluster)
  } else{
    # call profileColumn for each table's column
    profile$columnProfile <- lapply(columns.metadata$column_name,
                                    function(x) profileColumn(
                                      conn.info = conn.info,
                                      schema = schema,
                                      table = table,
                                      column = x,
                                      column.datatype =
                                        getColumnDatatype(x,
                                                          columns.metadata),
                                      query.filter = query.filter,
                                      format.show.percentage = format.show.percentage))
  }

  profile$endtime = Sys.time()
  print(paste0(profile$endtime," Ended profile at table ", schema, ".", table))
  return(profile)
}
