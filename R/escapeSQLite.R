escapeSQLite <- function(text){
  return(paste0("`", text, "`"))
}