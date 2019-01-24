buildQueryColumnStats <- function(conn.info, ...){
  UseMethod("buildQueryColumnStats", conn.info)
}

#' buildQueryColumnStats.sqlite
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(distinct column) from table
#' @export
buildQueryColumnStats.sqlite <- function(conn.info,
                                         schema,
                                         table,
                                         column,
                                         query.filter, ...){

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

#' buildQueryColumnStats.sqlserver
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(distinct column) from table
#' @export
buildQueryColumnStats.sqlserver <- function(conn.info,
                                            schema,
                                            table,
                                            column,
                                            query.filter, ...){

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

#' buildQueryColumnStats.teradata
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(distinct column) from table
#' @export
buildQueryColumnStats.teradata <- function(conn.info,
                                           schema,
                                           table,
                                           column,
                                           query.filter, ...){

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