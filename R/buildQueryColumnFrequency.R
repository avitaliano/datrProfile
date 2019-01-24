buildQueryColumnFrequency <- function(conn.info, ...){
  UseMethod("buildQueryColumnFrequency", conn.info)
}

buildQueryColumnFrequency.sqlite <- function(conn.info,
                                             schema,
                                             table,
                                             column,
                                             limit.freq.values,
                                             query.filter, ...){

  if (is.na(query.filter)){
    query <- paste("SELECT", escapeSQLite(column), "AS value,",
                   "COUNT(*) AS freq",
                   "FROM ", table,
                   "GROUP BY ", escapeSQLite(column),
                   "ORDER BY freq DESC, value",
                   "LIMIT", limit.freq.values)
  } else {
    query <- paste("SELECT", escapeSQLite(column), "AS value,",
                   "COUNT(*) AS freq",
                   "FROM ", table,
                   "WHERE", query.filter,
                   "GROUP BY ", escapeSQLite(column),
                   "ORDER BY freq DESC, value",
                   "LIMIT", limit.freq.values)
  }

  return(query)
}


buildQueryColumnFrequency.sqlserver <- function(conn.info,
                                                schema,
                                                table,
                                                column,
                                                limit.freq.values,
                                                query.filter, ...){
  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)) {
    query <- paste("SELECT TOP", limit.freq.values, column, "AS value",
                   ", COUNT(*) AS freq",
                   "FROM ", schema.table,
                   "GROUP BY ", column,
                   "ORDER BY freq DESC, value")
  } else {
    query <- paste("SELECT TOP", limit.freq.values, column, "AS value",
                   ", COUNT(*) AS freq",
                   "FROM ", schema.table,
                   "WHERE", query.filter,
                   "GROUP BY ", column,
                   "ORDER BY freq DESC, value")
  }
  return(query)
}


buildQueryColumnFrequency.teradata <- function(conn.info,
                                               schema,
                                               table,
                                               column,
                                               limit.freq.values,
                                               query.filter,
                                               ...){
  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)){
    query <- paste("SELECT TOP", limit.freq.values, column, "AS columnValue",
                   ", COUNT(*) AS freq",
                   "FROM ", schema.table,
                   "GROUP BY ", column,
                   "ORDER BY freq DESC, columnValue")
  } else{
    query <- paste("SELECT TOP", limit.freq.values, column, "AS columnValue",
                   ", COUNT(*) AS freq",
                   "FROM ", schema.table,
                   "WHERE", query.filter,
                   "GROUP BY ", column,
                   "ORDER BY freq DESC, columnValue")
  }
  return(query)
}