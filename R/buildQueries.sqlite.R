# buildQueries.sqlite

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

buildQueryCountTotal.sqlite <- function(conn.info, schema, table, ...){

  query <- paste("SELECT COUNT(*) FROM ", table)
  return(query)
}

buildQueryCountNull.sqlite <- function(conn.info, schema, table,
                                          column, ...){

  query <- paste("SELECT COUNT(*) FROM", table,
                 "WHERE", column, "IS NULL" )
  return(query)
}

buildQueryColumnStats.sqlite <- function(conn.info, schema, table,
                                            column, ...){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  # Count(distinct column), min(column), max(column) from table
  query <- paste("SELECT COUNT(DISTINCT ", column, " ),",
                 "MIN(", column, "),",
                 "MAX(", column, ")",
                 "FROM ", schema.table)
  return(query)
}

buildQueryColumnFrequency.sqlite <- function(conn.info,
                                                schema,
                                                table,
                                                column,
                                                limit.freq.values, ...){
  query <- paste("SELECT", column, "AS value,",
                 "COUNT(*) AS freq",
                 "FROM ", table,
                 "GROUP BY ", column,
                 "ORDER BY freq DESC, value",
                 "LIMIT", limit.freq.values)
  return(query)
}
