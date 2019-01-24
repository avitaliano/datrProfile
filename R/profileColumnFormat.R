#' profileColumnFormat
#'
#' Profiles column based on its format, using masking strategy.
#' X = char, 9 = digit, S = symbol
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param column Column name that will be profiled
#' @param column.datatype Column datatipe
#' @param schema Table schema
#' @param table Table name
#' @param count.total Number of rows to be profiled
#' @param query.filter Filter applied to the table, when profilling
#' @param show.percentage Threshold considered when showing formats'
#' percentages
#'
#' @return Data Frame containing columns' metadata
profileColumnFormat <- function(conn.info,
                          column,
                          column.datatype,
                          schema,
                          table,
                          count.total,
                          query.filter,
                          show.percentage = 0.01){

  # Not getting column format stats for datetime columns
  if( ! isDatetimeColumn(column.datatype) ){

    # builds query
    query.format.freq <- buildQueryProfileColumnFormatFrequency(conn.info,
                                                                column,
                                                                table,
                                                                schema,
                                                                query.filter)

    # only implemented in teradata database.
    if ( ! is.na(query.format.freq)) {

      # Connects to database
      conn <- connectDB(conn.info)

      format.freq <- odbc::dbGetQuery(conn, query.format.freq)
      names(format.freq) <- c( "format", "freq")

      closeConnection(conn)

      # calculate percentages
      format.freq$perc = format.freq$freq / count.total

      # only shows values with percentage is greater then (or equal)
      # show.percentage arg
      others <- format.freq[format.freq$perc < show.percentage,]

      if ( nrow(others) > 0 )
        format.freq <- rbind(
          format.freq[format.freq$perc >= show.percentage,],
          dplyr::summarize(others, format = "others",
                           freq = sum(freq),
                           perc = sum(perc))
          )

      return(format.freq)

    } else {
      return(NA)
    }
  } else { # if( ! is.datetimeColumn(column.datatype) ){
    return(NA)
  }
}