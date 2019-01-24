isLongIntColumn <- function(column.datatype){
  longInt_datatypes <- c("BIGINT", "INTEGER64")
  return(any(toupper(column.datatype) %in% longInt_datatypes))
}
