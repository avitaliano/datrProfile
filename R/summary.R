#print.profile

#' Shows a summary of the data profile
#'
#' @param object Profile object
#' @param ... other parameters
#'
#' @return data.frame with summary information
#' @export
summary.profile <- function(object, ...){

  # isolates data.frame from columnProfile list to become print-friendly
  f <- function(columnProfile){

    # remove column.freq and format.freq
    columnProfile$column.freq <- NULL
    columnProfile$format.freq <- NULL

    # to data frame
    df <- as.data.frame(columnProfile, stringsAsFactors=FALSE)
    rownames(df) <- NULL

    # convert datetime columns to char
    # because of automatic cast at min and max columns
    if ( isDatetimeColumn(class(df$min.value)) ) {
      df$min.value <- format(df$min.value, "%Y-%m-%d %H:%M:%S" )
    }

    if ( isDatetimeColumn(class(df$max.value)) ) {
      df$max.value <- format(df$max.value, "%Y-%m-%d %H:%M:%S" )
    }

    # convert bigint columns to char
    if ( isLongIntColumn(class(df$min.value)) ) {
      df$min.value <- as.character(df$min.value)
    }

    if ( isLongIntColumn(class(df$max.value)) ) {
      df$max.value <- as.character(df$max.value)
    }

    return(df)
  }

  summary <- do.call(rbind, lapply(object$columnProfile, f))
  return(summary)
}