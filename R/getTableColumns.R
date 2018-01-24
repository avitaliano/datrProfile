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

getColumnDatatype <- function(column, columns.metadata){
  return(columns.metadata[columns.metadata$column_name == column, ]$column_datatype)
}

is.stringColumn <- function(column.datatype){

  string_datatypes <- c("CHAR", "VARCHAR", "STRING", "TEXT")
  return(any(toupper(column.datatype) %in% string_datatypes))

}

is.datetimeColumn <- function(column.datatype){

  datetime_datatypes <- c("TIMESTAMP", "DATE", "TIME", "DATETIME",
                          "POSIXT", "POSIXCT")
  return(any(toupper(column.datatype) %in% datetime_datatypes))

}

is.numericColumn <- function(column.datatype){

  numeric_datatypes <- c("INT", "INTEGER", "NUMBER", "NUMERIC")
  return(any(toupper(column.datatype) %in% numeric_datatypes))

}