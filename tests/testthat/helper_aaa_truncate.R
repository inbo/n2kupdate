message("Truncate tables")
conn <- connect_ut_db()
truncate_public(conn)
DBI::dbDisconnect(conn)
