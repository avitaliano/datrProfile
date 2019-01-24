

#' Print method
#'
#' @param x profile object
#' @param ... other parameters
#'
#' @return printed profile
#' @export
print.profile <- function(x, ...){

  print("Column Profile")
  print(paste("Schema:", x$schema))
  print(paste("Table:", x$table))
  print(paste("Start time:", x$starttime))
  print(paste("End time:", x$endtime))
  print(summary(x))
}