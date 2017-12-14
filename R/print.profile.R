#print.profile

print.profile <- function(x, ...){

  print("Data Profile")
  print(paste("Schema:", x$schema))
  print(paste("Table:", x$table))
  print(paste("Start time:", x$starttime))
  print(paste("End time:", x$endtime))

  f <- function(columnProfile){

    is.datetimeColumn <- function(column.datatype){

      datetime_datatypes <- c("TIMESTAMP", "DATE", "TIME", "DATETIME",
                              "POSIXT", "POSIXCT")
      return(any(toupper(column.datatype) %in% datetime_datatypes))

    }

    # remove column.freq and format.freq
    columnProfile$column.freq <- NULL
    columnProfile$format.freq <- NULL

    # to data frame
    df <- as.data.frame(columnProfile)
    rownames(df) <- NULL

    # convert datetime columns to char
    # because of automatic cast at min and max columns

    if( is.datetimeColumn(class(df$min.value)) ){
      df$min.value <- format(df$min.value, "%Y-%m-%d %H:%M:%S" )
    }

    if( is.datetimeColumn(class(df$max.value)) ){
      df$max.value <- format(df$max.value, "%Y-%m-%d %H:%M:%S" )
    }

    return(df)
  }

  summary <- do.call(rbind, lapply(x$columnProfile, f))
  print(summary)
}