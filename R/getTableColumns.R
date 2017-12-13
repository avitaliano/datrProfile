# getTableColumns
getTableColumns <- function(conn.info, ...){
  UseMethod("getTableColumns", conn.info)
}

getTableColumns.sqlserver <- function(conn.info, schema, table){
  # connects to database
  conn <- connectDB(conn.info)

  # retrieves columns' metadata
  query.columns <- odbc::dbSendQuery(conn, .queryTableColumnMetadata)
  odbc::dbBind(query.columns, list(database, schema, table))
  columns.metadata <- odbc::dbFetch(query.columns)
  odbc::dbClearResult(query.columns)

  # disconnects
  conn <- closeConnection(conn)
  return(columns.metadata)
}

getTableColumns.teradata <- function(conn.info, schema, table){
  # connects to database
  conn <- connectDB(conn.info)

  # retrieves columns' metadata
  query.columns <- odbc::dbSendQuery(conn, .queryTableColumnMetadata)
  odbc::dbBind(query.columns, list(schema, table))
  columns.metadata <- odbc::dbFetch(query.columns)
  odbc::dbClearResult(query.columns)

  # disconnects
  conn <- closeConnection(conn)
  return(columns.metadata)
}


