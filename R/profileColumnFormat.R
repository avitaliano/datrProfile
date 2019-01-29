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
  if( isDatetimeColumn(column.datatype) ){
    return(NA)
  }

    # builds query
    query.format.freq <- buildQueryProfileColumnFormatFrequency(conn.info,
                                                                schema = schema,
                                                                table = table,
                                                                column = column,
                                                                query.filter = query.filter)

    # not implemented in some databases.
    if ( is.na(query.format.freq)) {
      return(NA)
    }

    # connects to database
    conn <- connectDB(conn.info)

    format.freq <- odbc::dbGetQuery(conn, query.format.freq)
    names(format.freq) <- c( "format", "freq")

    closeConnection(conn)

    # calculate percentages
    format.freq$perc = format.freq$freq / count.total

    # only shows values with percentage is greater then (or equal)
    # show.percentage arg
    others <- format.freq[format.freq$perc < show.percentage,]

    # excludes detailed rows from format.freq, and binds others
    if ( nrow(others) > 0 ){
      # group others in one row
      others <- dplyr::summarise(others, format = "others",
                                 freq = sum(freq),
                                 perc = sum(perc))
      # binds others to the data frame
      format.freq <- rbind(
        format.freq[format.freq$perc >= show.percentage,],
        others
      )
    }
    return(format.freq)
}