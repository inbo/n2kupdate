% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/store_n2kModel.R
\name{store_n2kModel}
\alias{store_n2kModel}
\title{extract the results from an n2kModel and stored them}
\usage{
store_n2kModel(x, conn, hash, clean = TRUE)
}
\arguments{
\item{x}{the n2kModel object}

\item{conn}{a DBIconnection}

\item{hash}{the hash of the update session}

\item{clean}{perform all database operations within a transaction and clean up the staging tables. Defaults to TRUE.}
}
\description{
extract the results from an n2kModel and stored them
}
