#' Prepares connection to RDBS via ODBC
#'
#' \code{prepareConnection} list connection details needed to connecto
#' to a RDBS using ODBC
#'
#' @param db.vendor Database vendor (teradata, sqlserver)
#' @param db.host Database hostname
#' @param db.name Database name
#' @param dsn Data source name
#' @param user Username to connect to database
#' @param passwd Password to connect to database
#' @export
#' @examples
#' connection.info <- prepareConnection(db.vendor = "teradata",
#'    dsn = "ODBC_MYDB", user = "myuser", passwd = "mypasswd")
prepareConnection <- function(db.vendor, db.host = NULL, db.name = NULL,
                               dsn = NULL, user = NULL, passwd = NULL){
  if (missing(db.vendor))
    stop("Missing db.vendor arg")

  connection.info <- list(db.host = db.host, db.name = db.name, dsn = dsn,
                          user = user, passwd = passwd)
  class(connection.info) <- db.vendor

  return(connection.info)
}

#' Connects to database using \code{\link{odbc::dbConnect}}
#'
#'
#' @param connection.info Connection info created at \code{\link{prepareConnection()}}
#' @return \code{channel} to database
#' @export
#' @examples
#' connection.info <- prepareConnection(db.vendor = "teradata",
#'    db.host = "192.168.0.36", user = "myuser", passwd = "mypasswd")
#' teradata.channel <- connectDB(connection.info)
connectDB <- function(connection.info){
  UseMethod("connectDB", connection.info)
}

connectDB <- function(conn){

  stop_msg <- paste("Database", class(conn), "not supported")
  stop(stop_msg)
}

#' @return \code{conn} to Teradata database connection
#'
#' @rdname connectDB
#' @method connectDB teradata
#' @S3method connectDB teradata
connectDB.teradata <- function(connection.info){

  # args check
  connection.info
  if (is.null(connection.info$dsn))
    stop("Data Source Name (DSN) needed to connect to teradata database.")

  if (is.null(connection.info$user))
    stop("Username needed to connect to teradata database.")

  if (is.null(connection.info$passwd))
    stop("User password needed to connect to teradata database.")

  if (! is.null(connection.info$db.host) )
    warning("db.host arg not used to connect. DSN used instead.")

  conn <- odbc::dbConnect(odbc::odbc(), dsn = connection.info$dsn,
                          uid = connection.info$user,
                          pwd = connection.info$passwd,
                          database = connection.info$db.name)
  return(conn)
}

#' @return \code{conn} to MS SQL Server database connection
#'
#' @rdname connectDB
#' @method connectDB sqlserver
#' @S3method connectDB sqlserver
connectDB.sqlserver <- function(connection.info){

  # TODO: Implement connection using RSqlServer drivers
  # dbConnect(RSQLServer::SQLServer(), server = "SQLServer")

  # args check
  if (is.null(connection.info$dsn))
    stop("Data Source Name (DSN) needed to connect to SQL Server.")

  if (! is.null(connection.info$db.host) )
    warning("db.host arg not used to connect. DSN used instead.")

  # Connects to database
  conn <- odbc::dbConnect(odbc::odbc(), dsn = connection.info$dsn,
                          uid = connection.info$user,
                          pwd = connection.info$passwd,
                          database = connection.info$db.name)
  return(conn)
}


#' Connects to database using \code{\link{odbc::dbConnect}}
#'
#'
#' @param connection.info Connection info created at \code{\link{prepareConnection()}}
#' @return \code{channel} to database
#' @export
#' @examples
#' connection.info <- prepareConnection(db.vendor = "teradata",
#'    db.host = "192.168.0.36", user = "myuser", passwd = "mypasswd")
#' teradata.channel <- connectDB(connection.info)
closeConnection <- function(conn){
  odbc::dbDisconnect(conn)
}