#' buildQueryCountNull
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection()}}
#' @param ... other parameters
#'
#' @return query count(null)
#' @export
buildQueryCountNull <- function(conn.info, ...){
  UseMethod("buildQueryCountNull", conn.info)
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