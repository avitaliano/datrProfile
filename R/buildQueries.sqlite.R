# buildQueries.sqlite

escapeSQLite <- function(text){
  return(paste0("`", text, "`"))
}

buildQueryColumnMetadata.sqlite <- function(conn.info,
                                            schema = NULL,
                                            table,
                                            ...){

  query <- paste("SELECT '' as table_schema,",
                 paste0( "'", table, "'"),
                 "as table_name,",
                 "name as column_name,",
                 "type as column_datatype,",
                 "0 AS column_length,",
                 "0 AS column_precision",
                 "FROM PRAGMA_TABLE_INFO(",
                 paste0( "'", table, "'"), ")")

  return(query)
}

buildQueryCountTotal.sqlite <- function(conn.info, schema, table,
                                        query.filter, ...){
  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM ", table)
  } else {
    query <- paste("SELECT COUNT(*) FROM ", table,
                   "WHERE", query.filter)
  }
  return(query)
}

buildQueryCountNull.sqlite <- function(conn.info, schema, table,
                                       column, query.filter,  ...){

  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM", table,
                   "WHERE", escapeSQLite(column), "IS NULL" )
  } else {
    query <- paste("SELECT COUNT(*) FROM", table,
                   "WHERE", escapeSQLite(column), "IS NULL",
                   "AND", query.filter)
  }
  return(query)
}

buildQueryColumnStats.sqlite <- function(conn.info, schema, table,
                                         column, query.filter, ...){

  # Count(distinct column), min(column), max(column) from table
  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(DISTINCT ", escapeSQLite(column), " ),",
                   "MIN(", escapeSQLite(column), "),",
                   "MAX(", escapeSQLite(column), ")",
                   "FROM ", table)
  } else {
    query <- paste("SELECT COUNT(DISTINCT ", escapeSQLite(column), " ),",
                   "MIN(", escapeSQLite(column), "),",
                   "MAX(", escapeSQLite(column), ")",
                   "FROM ", table,
                   "WHERE", query.filter)
  }

  return(query)
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

buildQueryProfileColumnFormatFrequency.sqlite <- function(conn.info,
                                                          column,
                                                          table,
                                                          schema,
                                                          ...){
  return(NA)
}

