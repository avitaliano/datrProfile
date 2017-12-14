# runProfile

#' Title
#'
#' @param conn.info
#' @param schema
#' @param table
#' @param is.parallel
#'
#' @return
#' @export
#'
#' @examples
runProfile <- function(conn.info, schema, table, is.parallel = FALSE){

  print(paste0("Starting profile at table ", schema, ".", table,
               " at ", Sys.time()))

  profile <- list( schema = schema,
                   table = table,
                   columnProfile = NULL,
                   starttime = Sys.time(),
                   endtime = NULL)

  # Starting Column Profile
  columns.metadata <- getTableColumns(conn.info, schema, table)

  if (is.parallel){

  }else{
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
