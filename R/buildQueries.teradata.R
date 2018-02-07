# buildQueries.teradata.R

buildQueryColumnMetadata.teradata <- function(conn.info,
                                              schema,
                                              table,
                                              ...){

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

buildQueryCountTotal.teradata <- function(conn.info,
                                          schema,
                                          table,
                                          query.filter,
                                          ...){

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

buildQueryCountNull.teradata <- function(conn.info,
                                         schema,
                                         table,
                                         column,
                                         query.filter,
                                         ...){

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

buildQueryColumnStats.teradata <- function(conn.info,
                                           schema,
                                           table,
                                           column,
                                           query.filter,
                                           ...){

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

buildQueryProfileColumnFormatFrequency.teradata <- function(conn.info,
                                                            column,
                                                            table,
                                                            schema,
                                                            query.filter,
                                                            ...){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)){
    query <- paste0("SELECT REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column,
                    "), '[A-Za-z]', 'X'),",
                    "'[ ]', 'b'), '[0-9]', '9') AS COLUMN_FORMAT,",
                    "COUNT(*) AS FREQ",
                    " FROM ", schema.table,
                    " GROUP BY REGEXP_REPLACE( ",
                    "REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column, "), '[A-Za-z]', 'X'), '[ ]', 'b'), '[0-9]', '9')",
                    " ORDER BY FREQ DESC"
    )
  }
  else {
    query <- paste0("SELECT REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column,
                    "), '[A-Za-z]', 'X'),",
                    "'[ ]', 'b'), '[0-9]', '9') AS COLUMN_FORMAT,",
                    "COUNT(*) AS FREQ",
                    " FROM ", schema.table,
                    " WHERE ", query.filter,
                    " GROUP BY REGEXP_REPLACE( ",
                    "REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column, "), '[A-Za-z]', 'X'), '[ ]', 'b'), '[0-9]', '9')",
                    " ORDER BY FREQ DESC"
    )
  }

  return(query)
}
