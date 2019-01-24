#' getTableColumns
#'
#' Issues query against the RDBS to retrieve information about each column
#' of the table. Name, type, length, precision, etc.
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection()}}
#' @param schema Table schema
#' @param table Table name
#'
#' @return data frame containing the columns' metadata
#' @export
getTableColumns <- function(conn.info, schema, table){

  # opens connection to db
  conn <- connectDB(conn.info)

  # builds query to retrieve columns metadata
  query.columns <- buildQueryColumnMetadata(conn.info,
                                            schema = schema,
                                            table = table)

  # issues the query
  columns.metadata <- odbc::dbGetQuery(conn, query.columns)

  # disconnects
  conn <- closeConnection(conn)
  return(columns.metadata)
}