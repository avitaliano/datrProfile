#' buildQueryColumnMetadata
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param ... Other params
#'
#' @return query columns' metadata
buildQueryColumnMetadata <- function(conn.info, ...){
  UseMethod("buildQueryColumnMetadata", conn.info)
}

#' buildQueryColumnMetadata.teradata
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param ... Other params
#'
#' @return query columns' metadata
#' @noRd
buildQueryColumnMetadata.teradata <- function(conn.info,
                                              schema,
                                              table, ...){

  query <-  paste("SELECT DatabaseName as table_schema,",
                  "TableName as table_name,",
                  "TRIM(ColumnName) as column_name,",
                  "CASE WHEN ColumnType = 'CV' THEN 'varchar'",
                  "WHEN ColumnType = 'CF' THEN 'char'",
                  "WHEN ColumnType = 'I' THEN 'int'",
                  "WHEN ColumnType = 'I2' THEN 'smalint'",
                  "WHEN ColumnType = 'I8' THEN 'bigint'",
                  "WHEN ColumnType = 'DA' THEN 'date'",
                  "WHEN ColumnType = 'TS' THEN 'datetime'",
                  "WHEN ColumnType = 'D' THEN 'decimal'",
                  "WHEN ColumnType = 'F' THEN 'float'",
                  "else ColumnType",
                  "end as column_datatype,",
                  "ColumnLength as column_length,",
                  "0 AS column_precision",
                  "FROM DBC.COLUMNS",
                  "WHERE DatabaseName =", paste0("'", schema, "'"),
                  "AND TableName =", paste0("'", table, "'"),
                  "ORDER BY ColumnID")
  return(query)
}

#' buildQueryColumnMetadata.sqlserver
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param database Database Name
#' @param ... Other params
#'
#' @return query columns' metadata
#' @noRd
buildQueryColumnMetadata.sqlserver <- function(conn.info,
                                               schema,
                                               table,
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

#' buildQueryColumnMetadata.sqlite
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param table  Table Name
#' @param ... Other params
#'
#' @return query columns' metadata
#' @noRd
buildQueryColumnMetadata.sqlite <- function(conn.info,
                                            table, ...){

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
