# load queries

## Constants

# get columns names
.queryTableColumnMetadata <- NULL

loadQueries <- function(conn.info){
  UseMethod("loadQueries", conn.info)
}

loadQueries.sqlserver <- function(conn.info){

  .queryTableColumnMetadata <<- paste("SELECT *",
                                      "SELECT TABLE_SCHEMA as table_schema,",
                                      "TABLE_NAME as table_name,",
                                      "COLUMN_NAME as column_name,",
                                      "DATA_TYPE as column_datatype,",
                                      "CASE WHEN DATA_TYPE IN ('varchar','char')",
                                      "THEN CHARACTER_MAXIMUM_LENGTH",
                                      "ELSE NUMERIC_PRECISION END AS column_length,",
                                      "CASE WHEN DATA_TYPE IN ('varchar', 'char') THEN 0",
                                      "ELSE NUMERIC_PRECISION_RADIX END AS column_precision",
                                      "FROM INFORMATION_SCHEMA.COLUMNS",
                                      "WHERE TABLE_CATALOG = ? AND",
                                      "TABLE_SCHEMA = ? AND TABLE_NAME = ?")
}

loadQueries.teradata <- function(conn.info){

  .queryTableColumnMetadata <<-  paste("SELECT DatabaseName as table_schema,",
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
                                       "WHERE DatabaseName = ?",
                                       "AND TableName = ?")
}
