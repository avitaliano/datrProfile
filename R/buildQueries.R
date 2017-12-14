# buildQueries

# Select column's metadata from db catalog
buildQueryColumnMetadata <- function(conn.info, ...){
  UseMethod("buildQueryColumnMetadata", conn.info)
}

# Count(*) from table
buildQueryCountTotal <- function(conn.info, ...){
  UseMethod("buildQueryCountTotal", conn.info)
}

# Count(*) from table column is null
buildQueryCountNull <- function(conn.info, ...){
  UseMethod("buildQueryCountNull", conn.info)
}

# Count(distinct column), min(column), max(column) from table
buildQueryColumnStats <- function(conn.info, ...){
  UseMethod("buildQueryColumnStats", conn.info)
}

buildQueryColumnFrequency <- function(conn.info, ...){
  UseMethod("buildQueryColumnFrequency", conn.info)
}
