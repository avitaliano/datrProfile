#' Prepares connection to RDBS via ODBC
#'
#' \code{prepareConnection} list connection details needed to connecto
#' to a RDBS using ODBC
#'
#' @param db.vendor Database vendor (teradata, sqlserver)
#' @param odbc.driver ODBC driver used to connect to database
#' @param db.host Database hostname
#' @param db.name Database name
#' @param db.encoding Database encoding
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
                              db.encoding = "",
                              dsn = NULL,
                              user = NULL,
                              passwd = NULL){
  if (missing(db.vendor))
    stop("Missing db.vendor arg")

  # create list with all arguments
  conn.info <- list(odbc.driver = odbc.driver,
                    db.host = db.host,
                    db.name = db.name,
                    db.encoding = db.encoding,
                    dsn = dsn,
                    user = user,
                    passwd = passwd)

  # vendor is used as class to call correct S3 method.
  class(conn.info) <- db.vendor
  return(conn.info)
}