# buildQueries.sqlserver

buildQueryColumnMetadata.sqlserver <- function(conn.info, schema, table,
                                               database, ...){

  query <- paste("SELECT TABLE_SCHEMA as table_schema,",
                 "TABLE_NAME as table_name,",
                 "COLUMN_NAME as column_name,",
                 "DATA_TYPE as column_datatype,",
                 "CASE WHEN DATA_TYPE IN ('varchar','char')",
                 "THEN CHARACTER_MAXIMUM_LENGTH",
                 "ELSE NUMERIC_PRECISION END AS column_length,",
                 "CASE WHEN DATA_TYPE IN ('varchar', 'char') THEN 0",
                 "ELSE NUMERIC_PRECISION_RADIX END AS column_precision",
                 "FROM INFORMATION_SCHEMA.COLUMNS",
                 "WHERE TABLE_SCHEMA =", paste0("'", schema, "'"),
                 "AND TABLE_NAME =", paste0("'", table, "'"))

  if ( !missing(database) )
    query <- paste(query, "AND TABLE_CATALOG =", database)

  return(query)
}

buildQueryCountTotal.sqlserver <- function(conn.info, schema,
                                           table, query.filter, ...){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM ", schema.table)
  } else{
    query <- paste("SELECT COUNT(*) FROM ", schema.table,
                   "WHERE", query.filter)
  }
  return(query)
}

buildQueryCountNull.sqlserver <- function(conn.info, schema, table,
                                          column, query.filter, ...){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM", schema.table,
                   "WHERE", column, "IS NULL" )
  } else {
    query <- paste("SELECT COUNT(*) FROM", schema.table,
                 "WHERE", column, "IS NULL",
                 "AND", query.filter)
  }
  return(query)
}

buildQueryColumnStats.sqlserver <- function(conn.info, schema, table,
                                            column, query.filter, ...){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  # Count(distinct column), min(column), max(column) from table
  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(DISTINCT ", column, " ),",
                   "MIN(", column, "),",
                   "MAX(", column, ")",
                   "FROM ", schema.table)
  } else{
    query <- paste("SELECT COUNT(DISTINCT ", column, " ),",
                   "MIN(", column, "),",
                   "MAX(", column, ")",
                   "FROM ", schema.table,
                   "WHERE", query.filter)
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

buildQueryProfileColumnFormatFrequency.sqlserver <- function(conn.info,
                                                             column,
                                                             table,
                                                             schema,
                                                             ...){
  return(NA)
}
