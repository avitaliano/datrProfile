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
#'    dsn = "ODBC_MYDB",)
prepare_connection <- function(db.vendor, db.host = NULL, db.name = NULL,
                               dsn = NULL, user = NULL, passwd = NULL){

  if (missing(db.vendor))
    stop("Missing db.vendor arg")

  connection.info <- list(db.host = db.host, db.name = db.name, dsn = dsn,
                          user = user, passwd = passwd)
  class(connection.info) <- db.vendor

  return(connection.info)
}
