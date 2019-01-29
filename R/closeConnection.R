#' closeConnection
#'
#' Disconnects from database using \code{\link[odbc]{dbDisconnect}}
#
#' @param conn Connection created at \code{\link{connectDB}}
#' @return \code{TRUE} if succeeded at closing connection
#' @export
closeConnection <- function(conn){
  return(odbc::dbDisconnect(conn))
}