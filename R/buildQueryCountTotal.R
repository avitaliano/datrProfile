#' buildQueryCountTotal
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param ... other parameters
#'
#' @return query count(*)
#' @export
buildQueryCountTotal <- function(conn.info, ...){
  UseMethod("buildQueryCountTotal", conn.info)
}

buildQueryCountTotal.sqlite <- function(conn.info, schema, table,
                                        query.filter, ...){
  if (is.na(query.filter)){
    query <- paste("SELECT COUNT(*) FROM ", table)
  } else {
    query <- paste("SELECT COUNT(*) FROM ", table,
                   "WHERE", query.filter)
  }
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