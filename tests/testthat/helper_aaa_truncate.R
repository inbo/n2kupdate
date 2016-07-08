conn <- connect_db()
truncate_public(conn)
DBI::dbDisconnect(conn)
