#' buildQueryProfileColumnFormatFrequency
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param ... Other parameters
#'
#' @return queries column format frequency from table
#' @export
buildQueryProfileColumnFormatFrequency <- function(conn.info, ...){
  UseMethod("buildQueryProfileColumnFormatFrequency", conn.info)
}

#' buildQueryProfileColumnFormatFrequency.sqlite
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return queries column format frequency from table
#' @export
buildQueryProfileColumnFormatFrequency.sqlite <- function(conn.info,
                                                          schema,
                                                          table,
                                                          column,
                                                          query.filter, ...){
  return(NA)
}

#' buildQueryProfileColumnFormatFrequency.teradata
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return queries column format frequency from table
#' @export
buildQueryProfileColumnFormatFrequency.teradata <- function(conn.info,
                                                            schema,
                                                            table,
                                                            column,
                                                            query.filter, ...){
  #TODO: handle accentuation

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  if (is.na(query.filter)){
    query <- paste0("SELECT REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column,
                    "), '[A-Za-z]', 'X'),",
                    "'[ ]', 'b'), '[0-9]', '9') AS COLUMN_FORMAT,",
                    "COUNT(*) AS FREQ",
                    " FROM ", schema.table,
                    " GROUP BY REGEXP_REPLACE( ",
                    "REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column, "), '[A-Za-z]', 'X'), '[ ]', 'b'), '[0-9]', '9')",
                    " ORDER BY FREQ DESC"
    )
  }
  else {
    query <- paste0("SELECT REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column,
                    "), '[A-Za-z]', 'X'),",
                    "'[ ]', 'b'), '[0-9]', '9') AS COLUMN_FORMAT,",
                    "COUNT(*) AS FREQ",
                    " FROM ", schema.table,
                    " WHERE ", query.filter,
                    " GROUP BY REGEXP_REPLACE( ",
                    "REGEXP_REPLACE( REGEXP_REPLACE(TRIM(",
                    column, "), '[A-Za-z]', 'X'), '[ ]', 'b'), '[0-9]', '9')",
                    " ORDER BY FREQ DESC"
    )
  }

  return(query)
}

#' buildQueryProfileColumnFormatFrequency.sqlserver
#'
#' @param conn.info Connection info created with \code{\link{prepareConnection}}
#' @param schema Table Schema
#' @param table  Table Name
#' @param column Column profiled
#' @param query.filter Filter applied to the profile
#' @param ... Other parameters
#'
#' @return queries column format frequency from table
#' @export
buildQueryProfileColumnFormatFrequency.sqlserver <- function(conn.info,
                                                             schema,
                                                             table,
                                                             column,
                                                             query.filter, ...){
  #TODO: there must be some other way...
  #TODO: handle symbols

  # Concat schema and table
  schema.table <- paste0(trimws(schema), ".", table)

  SELECT <-   paste("SELECT
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(", column, ", 'A', 'X'),    'B', 'X'),
  'C', 'X'),  'D', 'X'),  'E', 'X'),  'F', 'X'),  'G', 'X'), 'H', 'X'),
  'I', 'X'),'J', 'X'),'K', 'X'),'L', 'X'),'M', 'X'),'N', 'X'),'O', 'X'),
  'P', 'X'),'Q', 'X'),'R', 'X'),'S', 'X'),'T', 'X'), 'U', 'X'), 'V', 'X'),
  'W', 'X'),'Y', 'X'),'Z', 'X'), 'Á', 'X'), 'Ã', 'X'),'À', 'X'),'Â', 'X'),
  'É', 'X'),'Ê', 'X'),'È', 'X'), 'Í', 'X'),'Ì', 'X'), 'Ó', 'X'), 'Ò',
  'X'),'Õ', 'X'),'Ô', 'X'), 'Ú', 'X'), 'Ù', 'X'),'Û', 'X'), 'Ç', 'X'),
  ' ', 'b'), '0', '9'), '1', '9'), '2', '9'), '3', '9'), '4', '9'),
  '5', '9'), '6', '9'), '7', '9'), '8', '9'), ' ', '9')
  AS COLUMN_FORMAT
  , COUNT(*) AS FREQ
  FROM")

  GROUP_BY = paste(" GROUP BY
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(  REPLACE(
  REPLACE(  REPLACE(  REPLACE(  REPLACE(", column, ", 'A', 'X'),    'B', 'X'),
  'C', 'X'),  'D', 'X'),  'E', 'X'),  'F', 'X'),  'G', 'X'), 'H', 'X'),
  'I', 'X'),'J', 'X'),'K', 'X'),'L', 'X'),'M', 'X'),'N', 'X'),'O', 'X'),
  'P', 'X'),'Q', 'X'),'R', 'X'),'S', 'X'),'T', 'X'), 'U', 'X'), 'V', 'X'),
  'W', 'X'),'Y', 'X'),'Z', 'X'), 'Á', 'X'), 'Ã', 'X'),'À', 'X'),'Â', 'X'),
  'É', 'X'),'Ê', 'X'),'È', 'X'), 'Í', 'X'),'Ì', 'X'), 'Ó', 'X'), 'Ò',
  'X'),'Õ', 'X'),'Ô', 'X'), 'Ú', 'X'), 'Ù', 'X'),'Û', 'X'), 'Ç', 'X'),
  ' ', 'b'), '0', '9'), '1', '9'), '2', '9'), '3', '9'), '4', '9'),
  '5', '9'), '6', '9'), '7', '9'), '8', '9'), ' ', '9')
  ORDER BY 2 DESC")

  if (is.na(query.filter)){
    query <- paste(SELECT, schema.table, GROUP_BY )
  } else{
    query <- paste(SELECT, schema.table, " WHERE ", query.filter, GROUP_BY)
  }
}
