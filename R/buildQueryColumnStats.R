# Count(distinct column), min(column), max(column) from table
buildQueryColumnStats <- function(conn.info, ...){
  UseMethod("buildQueryColumnStats", conn.info)
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