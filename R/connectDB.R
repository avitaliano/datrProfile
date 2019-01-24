#' connectDB
#'
#' Connects to database using \code{\link{odbc::dbConnect}}
#'#'
#' @param conn.info Connection info created at \code{\link{prepareConnection()}}
#' @return \code{connection} to database
#' @export
#' @examples
#' conn.info.teradata <- prepareConnection(db.vendor = "teradata",
#'    db.encoding = "latin1"
#'    db.host = "192.168.0.36",
#'    user = "myuser", passwd = "mypasswd")
#' conn <- connectDB(conn.info.teradata)
#' conn.info.sqlite <- prepareConnection(db.vendor = "sqlite",
#'      db.name = "/path/mydb.sqlite")
#' conn <- connectDB(conn.info.sqlite)
connectDB <- function(conn.info, ...){
  UseMethod("connectDB", conn.info)
}


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

connectDB.sqlite <- function(conn.info, ...){

  conn <- odbc::dbConnect(conn.info$odbc.driver,
                          conn.info$db.name,
                          ...)
  return(conn)
}


