# Profile the format of the column from the schema.table
profileColumnFormat <- function(conn.info,
                          column,
                          column.datatype,
                          schema,
                          table,
                          count.total,
                          query.filter,
                          show.percentage = 0.01){

  # does not get column format stats for datetime columns
  if( ! is.datetimeColumn(column.datatype) ){

    # builds query
    query.format.freq <- appendWhereClause(
      buildQueryProfileColumnFormatFrequency(conn.info,
                                             column,
                                             table,
                                             schema),
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