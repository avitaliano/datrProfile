# buildQueries

# Count(*) from table
buildQueryCountTotal <- function(conn.info, ...){
  UseMethod("buildQueryCountTotal", conn.info)
}

buildQueryCountTotal.teradata <- function(conn.info, schema, table){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  query <- paste("SELECT COUNT(*) AS CountTotal FROM ", schema.table)
  return(query)
}

buildQueryCountTotal.sqlserver <- function(conn.info, schema, table){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  query <- paste("SELECT COUNT(*) AS CountTotal FROM ", schema.table)
  return(query)
}

# Count(distinct column), min(column), max(column) from table
buildQueryColumnStats <- function(conn.info, ...){
  UseMethod("buildQueryColumnStats", conn.info)
}

buildQueryColumnStats.teradata <- function(conn.info, schema, table, column){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  # Count(distinct column), min(column), max(column) from table
  query <- paste("SELECT COUNT(DISTINCT ", column, " ),",
                 "MIN(", column, "),",
                 "MAX(", column, ") ",
                 "FROM", schema.table,
                 "WHERE", column, "IS NOT NULL")
  return(query)
}

buildQueryColumnStats.sqlserver <- function(conn.info, schema, table, column){

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  # Count(distinct column), min(column), max(column) from table
  query <- paste("SELECT COUNT(DISTINCT ", column, " ),",
                  "MIN(", column, "),",
                  "MAX(", column, ")",
                  "FROM ", schema.table)
  return(query)
}

