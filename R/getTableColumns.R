# getTableColumns

getTableColumns <- function(conn.info, schema, table){
  # connects to database
  conn <- connectDB(conn.info)

  # retrieves columns' metadata
  query.columns <- buildQueryColumnMetadata(conn.info,
                                            schema = schema,
                                            table = table)
  columns.metadata <- odbc::dbGetQuery(conn, query.columns)

  # disconnects
  conn <- closeConnection(conn)
  return(columns.metadata)
}
