#' connectDB
#'
#' Connects to database using \code{\link{odbc:dbConnect}}
#'
#' @param conn.info Connection info created at \code{\link{prepareConnection}}
#' @param ... Other parameters
#' @return \code{connection} to database
#' @export
connectDB <- function(conn.info, ...){
  UseMethod("connectDB", conn.info)
}


#' connectDB.sqlite
#'
#' @param conn.info Connection info created at \code{\link{prepareConnection}}
#' @param ... Other parameters
#'
#' @return \code{connection} to database
#' @export
connectDB.sqlite <- function(conn.info, ...){
  conn <- odbc::dbConnect(RSQLite::SQLite(), conn.info$db.name)
  return(conn)
}


#' connectDB.default
#'
#' @param conn.info Connection info created at \code{\link{prepareConnection}}
#' @param ... Other parameters
#'
#' @return \code{connection} to database
#' @export
connectDB.default <- function(conn.info, ...){

  conn <- odbc::dbConnect(conn.info$odbc.driver,
                          dsn = conn.info$dsn,
                          uid = conn.info$user,
                          pwd = conn.info$passwd,
                          database = conn.info$db.name,
                          host = conn.info$db.host,
                          encoding = conn.info$db.encoding,
                          ...)
  return(conn)
}



