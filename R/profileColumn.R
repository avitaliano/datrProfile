# profileColumn

profileColumn <- function(conn.info,
                          column,
                          table,
                          schema,
                          limit.freq.values = 30){

  # TODO: implement filtered profile

  cat(sprintf("Starting statistics for column %s - at %s\n",
                column,
                Sys.time()))

  # Connects to database
  conn <- connectDB(conn.info)

  # Count(*) from table
  query.count.total <- buildQueryCountTotal(conn.info,
                                           schema = schema,
                                           table = table)
  count.total <- unlist(odbc::dbGetQuery(conn, query.count.total))

  # Count(distinct column), min(column), max(column) from table
  query.column.stats <- buildQueryColumnStats(conn.info,
                                              schema,
                                              table,
                                              column)
  column.stats <- odbc::dbGetQuery(conn, query.column.stats)

  count.distinct <- column.stats[[1]]
  min.value <- column.stats[[2]]
  max.value <- column.stats[[3]]

  # Count(*) from table where column is null
  query.count.null <- buildQueryCountNull(conn.info,
                                          schema,
                                          table,
                                          column)
  count.null <- unlist(odbc::dbGetQuery(conn, query.count.null))

  # Select values and frequencies
  # Column, count(*) from table group by column
  query.column.freq <- buildQueryColumnFrequency(conn.info,
                                                 schema,
                                                 table,
                                                 column,
                                                 limit.freq.values)
  column.freq <- odbc::dbGetQuery(conn, query.column.freq)
  names(column.freq) <- c( "value", "freq")
  column.freq$perc <- column.freq$freq / count.total

  # closes connection
  closeConnection(conn)

  # Percentage stats
  perc.distinct = count.distinct / count.total
  perc.null = count.null / count.total

  columnProfile <- list(column = column,
                  count.total = unclass(count.total),
                  count.distinct = count.distinct,
                  perc.distinct = perc.distinct,
                  count.null = count.null,
                  perc.null = perc.null,
                  min.value = min.value,
                  max.value = max.value,
                  column.freq = column.freq)

  rownames(columnProfile) <- NULL

  cat(sprintf("Ended statistics for column %s - at %s\n",
              column,
              Sys.time()))

  return(columnProfile)
}
