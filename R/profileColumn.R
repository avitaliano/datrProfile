# profileColumn

profileColumn <- function(conn.info, column, table, schema){

    cat(sprintf("Starting statistics for column %s - at %s\n",
                column,
                Sys.time()))

  # connects to database
  conn <- connectDB(conn.info)

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  # Count(*) from table
  .queriCountTotal <- buildQueryCountTotal(conn.info,
                                           schema = schema,
                                           table = table)
  count.total <- unlist(odbc::dbGetQuery(conn, .queriCountTotal))

  # Count(distinct column), min(column), max(column) from table
  # .queryColumnStats <- paste0("SELECT COUNT(DISTINCT ", column, " ), ",
  #                               "MIN(", column, "), ",
  #                               "MAX(", column, ") ",
  #                               "FROM ", schema.table)
  .queryColumnStats <- buildQueryColumnStats(conn.info, schema, table, column)
  column.stats <- odbc::dbGetQuery(conn, .queryColumnStats)

  count.distinct <- column.stats[[1]]
  min.value <- column.stats[[2]]
  max.value <- column.stats[[3]]

  # Count(*) from table where column is null
  .queryCountNull <- paste("SELECT COUNT(*) FROM", schema.table,
                           "WHERE", column, "IS NULL" )
  count.null <- unlist(odbc::dbGetQuery(conn, .queryCountNull))

  # closes connection
  closeConnection(conn)

  # Percentage stats
  perc.distinct = count.distinct / count.total
  perc.null = count.null / count.total

  columnProfile <- data.frame(column = column,
                  count.total = unclass(count.total),
                  count.distinct = count.distinct,
                  perc.distinct = perc.distinct,
                  count.null = count.null,
                  perc.null = perc.null,
                  min.value = min.value,
                  max.value = max.value,
                  stringsAsFactors=FALSE)

  rownames(columnProfile) <- NULL
  print(columnProfile)

  # cat(sprintf("Ended statistics for column %s - at %s\n",
  #             column,
  #             Sys.time()))

  return(columnProfile)
}