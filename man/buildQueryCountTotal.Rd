% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/buildQueryCountTotal.R
\name{buildQueryCountTotal}
\alias{buildQueryCountTotal}
\title{buildQueryCountTotal}
\usage{
buildQueryCountTotal(conn.info, ...)
}
\arguments{
\item{conn.info}{Connection info created with \code{\link{prepareConnection}}}

\item{...}{Other params}
}
\value{
query count(*) from table
}
\description{
Count total rows from table.
}
