#' Prepares connection to RDBS via ODBC
#'
#' \code{prepareConnection} list connection details needed to connecto
#' to a RDBS using ODBC
#'
#' @param db.vendor Database vendor (teradata, sqlserver)
#' @param odbc.driver ODBC driver used to connect to database
#' @param db.host Database hostname
#' @param db.name Database name
#' @param dsn Data source name
#' @param user Username to connect to database
#' @param passwd Password to connect to database
#' @export
#' @examples
#' conn.info <- prepareConnection(db.vendor = "teradata",
#'    dsn = "ODBC_MYDB", user = "myuser", passwd = "mypasswd")
prepareConnection <- function(db.vendor,
                              odbc.driver = odbc::odbc(),
                              db.host = NULL,
                              db.name = NULL,
                              dsn = NULL,
                              user = NULL,
                              passwd = NULL){
  if (missing(db.vendor))
    stop("Missing db.vendor arg")

  # create list with all arguments
  conn.info <- list(odbc.driver = odbc.driver,
                          db.host = db.host,
                          db.name = db.name,
                          dsn = dsn,
                          user = user,
                          passwd = passwd)

  # vendor is used as class to call correct S3 method.
  class(conn.info) <- db.vendor
  return(conn.info)
}

#' Connects to database using \code{\link{odbc::dbConnect}}
#'#'
#' @param conn.info Connection info created at \code{\link{prepareConnection()}}
#' @return \code{connection} to database
#' @examples
#' conn.info <- prepareConnection(db.vendor = "teradata",
#'    db.host = "192.168.0.36", user = "myuser", passwd = "mypasswd")
#' conn <- connectDB(conn.info)
connectDB <- function(conn.info){

  # TODO: handle encoding issues
  conn <- odbc::dbConnect(conn.info$odbc.driver,
                          dsn = conn.info$dsn,
                          uid = conn.info$user,
                          pwd = conn.info$passwd,
                          database = conn.info$db.name,
                          host = conn.info$db.host)
  return(conn)
}

#' Disconnnects from database using \code{\link{odbc::dbDisconnect}}
#'#'
#' @param conn Connection created at \code{\link{connectDB()}}
#' @return \code{TRUE} if succeeded at closing connection
closeConnection <- function(conn){
  return(odbc::dbDisconnect(conn))
}
