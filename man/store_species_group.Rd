% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/store_species_group.R
\name{store_species_group}
\alias{store_species_group}
\title{Store species groups}
\usage{
store_species_group(species_group, hash, conn, clean = TRUE)
}
\arguments{
\item{species_group}{the data.frame with species groups. Must contains local_id, description and scheme. Other variables are ignored. local_id must have unique values.}

\item{hash}{the hash of the update session}

\item{conn}{a DBIconnection}

\item{clean}{perform all database operations within a transaction and clean up the staging tables. Defaults to TRUE.}
}
\description{
Store species groups
}
