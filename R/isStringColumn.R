isStringColumn <- function(column.datatype){

  string_datatypes <- c("CHAR", "VARCHAR", "STRING", "TEXT")
  return(any(toupper(column.datatype) %in% string_datatypes))

}