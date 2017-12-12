# load queries

## Constants

# get columns names
.queryTableColumnMetadata <- NULL

loadQueries <- function(conn.info){
  UseMethod("loadQueries", conn.info)
}

loadQueries.sqlserver <- function(conn.info){

  .queryTableColumnMetadata <<- paste("SELECT *",
                                  "FROM INFORMATION_SCHEMA.COLUMNS",
                                  "WHERE TABLE_CATALOG = ? AND",
                                  "TABLE_SCHEMA = ? AND TABLE_NAME = ?")
}

loadQueries.teradata <- function(conn.info){

  .queryTableColumnMetadata <<-  paste("SELECT * ",
                                       "FROM DBC.COLUMNS",
                                       "WHERE DatabaseName = ?",
                                       "AND TableName = ?")
}
