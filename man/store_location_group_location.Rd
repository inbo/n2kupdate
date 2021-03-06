% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/store_location_group_location.R
\name{store_location_group_location}
\alias{store_location_group_location}
\title{store the link between locations and location groups in the database}
\usage{
store_location_group_location(location_group_location, location_group,
  location, datafield, conn, hash, clean = TRUE)
}
\arguments{
\item{location_group_location}{a data.frame with the locations per location group. Must contain location_group_local_id and location_local_id. Other columns are ignored.}

\item{location_group}{the data.frame with location groups. Must contains local_id, description and scheme. Other variables are ignored. local_id must have unique values.}

\item{location}{a data.frame with location metadata. Must contain the following columns: local_id, description, parent_local_id, datafield_local_id and external_code. Other columns are ignored.}

\item{datafield}{a data.frame with datafield metadata. Must contain the variables local_id, datasource, table_name, primary_key and datafield_type. Other variables are ignored.}

\item{conn}{a DBIconnection}

\item{hash}{the hash of the update session}

\item{clean}{perform all database operations within a transaction and clean up the staging tables. Defaults to TRUE.}
}
\description{
store the link between locations and location groups in the database
}
\details{
\itemize{
 \item location_group_location must have variables location_local_id and location_group_local_id.
 \item location_group must have variables local_id, description and scheme
 \item location must have variables local_id, description, parent_local_id, datafield_local_id and extranal_code. Other variables are ignored
 \item datafield must have variables local_id, datasource, table_name, primary_key and datafield_type
 \item all local_id variables must be unique within their data.frame
 \item all values in location$datafield_local_id must exist in datafield$local_id
 \item all values in location$parent_location must be either NA or exist in location$local_id
 \item all values in location_group_location$location_local_id must exist in location$local_id
 \item all values in location_group_location$location_group_local_id must exist in location_group$local_id
}
}
