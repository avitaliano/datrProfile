isNumericColumn <- function(column.datatype){

  numeric_datatypes <- c("INT", "INTEGER", "NUMBER", "NUMERIC")
  return(any(toupper(column.datatype) %in% numeric_datatypes))

}