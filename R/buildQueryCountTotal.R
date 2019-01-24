buildQueryCountTotal <- function(conn.info, ...){
  UseMethod("buildQueryCountTotal", conn.info)
}

#' buildQueryCountTotal.sqlite
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(*) from table
#' @export
buildQueryCountTotal.sqlite <- function(conn.info,
                                        schema,
                                        table,
                                        query.filter, ...){
  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM ", table)
  } else {
    query <- paste("SELECT COUNT(*) FROM ", table,
                   "WHERE", query.filter)
  }
  return(query)
}

#' buildQueryCountTotal.sqlserver
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(*) from table
#' @export
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

#' buildQueryCountTotal.teradata
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return query count(*) from table
#' @export
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