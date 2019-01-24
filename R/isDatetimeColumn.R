isDatetimeColumn <- function(column.datatype){

  datetime_datatypes <- c("TIMESTAMP", "DATE", "TIME", "DATETIME",
                          "POSIXT", "POSIXCT")
  return(any(toupper(column.datatype) %in% datetime_datatypes))

}