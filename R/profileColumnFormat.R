# profileColumnFormat

is.datetimeColumn <- function(column.datatype){

  datetime_datatypes <- c("TIMESTAMP", "DATE", "TIME", "DATETIME",
                          "POSIXT", "POSIXCT")
  return(any(toupper(column.datatype) %in% datetime_datatypes))
}

# Profile the format of the column from the schema.table
profileColumnFormat <- function(conn.info,
                          column,
                          column.datatype,
                          schema,
                          table,
                          count.total){

  # does not get column format stats for datetime columns
  if( ! is.datetimeColumn(column.datatype) ){

    # Connects to database
    conn <- connectDB(conn.info)

    # builds query
    query.format.freq <- buildQueryProfileColumnFormatFrequency(conn.info,
                                                                column,
                                                                table,
                                                                schema)

    # only implemented in teradata database.
    if ( ! is.na(query.format.freq)) {

      format.freq <- odbc::dbGetQuery(conn, query.format.freq)
      names(format.freq) <- c( "format", "freq")

      closeConnection(conn)

      # calculate percentages
      format.freq$perc = format.freq$freq / count.total
      return(format.freq)

    } else {
      return(NA)
    }
  } else { # if( ! is.datetimeColumn(column.datatype) ){
    return(NA)
  }
}