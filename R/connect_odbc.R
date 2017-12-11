#' Prepares connection to RDBS via ODBC
#'
#' \code{prepare_connection} list connection details needed to connecto
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
#' connection.info <- prepare_connection(db.vendor = "teradata",
#'    db.host = "192.168.0.36", user = "myuser", passwd = "mypasswd")
#'
#' connection.info <- prepare_connection(db.vendor = "sqlserver",
#'    dsn = "ODBC_MYDB")
prepare_connection <- function(db.vendor, db.host = NULL, db.name = NULL,
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
#' @param connection.info Connection info created at \code{\link{prepare_connection}}
#' @return \code{channel} to database
#' @export
#' @examples
#' connection.info <- prepare_connection(db.vendor = "teradata",
#'    db.host = "192.168.0.36", user = "myuser", passwd = "mypasswd")
#' teradata.channel <- connect_db(connection.info)
connect_db <- function(connection.info){
  UseMethod("connect_db", connection.info)
}


#' @return \code{channel} to Teradata database connection
#'
#' @rdname connect_db
#' @method connect_db teradata
#' @S3method connect_db teradata
connect_db.teradata <- function(connection.info){

  # args check
  connection.info
  if (is.null(connection.info$db.host))
    stop("Teradata DCBName needed to connect to teradata database.")
  if (is.null(connection.info$user))
    stop("Username needed to connect to teradata database.")
  if (is.null(connection.info$passwd))
    stop("User password needed to connect to teradata database.")

  # Builds connection string
  connection.string <- paste0("Driver=Teradata;DBCName=",
                              connection.info$db.host,
                              ";UID=", connection.info$user,
                              ";PWD=", connection.info$passwd)

  #TODO(arnaldo): waiting for firewall allow to test dbconnect
  #channel <-
  #return(channel)
  return(NULL)
}

#' @return \code{channel} to MS SQL Server database connection
#'
#' @rdname connect_db
#' @method connect_db sqlserver
#' @S3method connect_db sqlserver
connect_db.sqlserver <- function(connection.info){

  # args check
  if (is.null(connection.info$dsn))
    stop("DSN needed to connect to SQL Server.")

  # Builds connection string
  connection.string <- paste0("DSN=", connection.info$dsn)

  if (!is.null(connection.info$user))
    connection.string <- paste0(connection.string,
                                ";UID=", connection.info$user,
                                ";PWD=", connection.info$passwd)

  if( !is.null(connection.info$db.name) )
    connection.string <- paste0(connection.string,
                                ";DATABASE=", connection.info$db.name)

  # Connects to database
  channel <- odbcDriverConnect(connection.string, readOnlyOptimize = TRUE)

  return(channel)

}

library(odbc)
ch <- dbConnect()
