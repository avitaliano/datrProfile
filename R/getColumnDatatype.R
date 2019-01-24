getColumnDatatype <- function(column, columns.metadata){
  return(columns.metadata[columns.metadata$column_name == column, ]$column_datatype)
}