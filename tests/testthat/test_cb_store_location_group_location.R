context("store_location_group_location")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
ut.location_group <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "id"],
  stringsAsFactors = FALSE
)
DBI::dbDisconnect(conn)

test_that("subfunction works correctly", {
  conn <- connect_db()

  # location_group
  expect_error(
    ut.location_group %>%
      select_(~-local_id) %>%
      store_location_group(conn = conn),
    "location_group does not have name local_id"
  )
  expect_error(
    ut.location_group %>%
      select_(~-description) %>%
      store_location_group(conn = conn),
    "location_group does not have name description"
  )
  expect_error(
    ut.location_group %>%
      select_(~-scheme) %>%
      store_location_group(conn = conn),
    "location_group does not have name scheme"
  )
  expect_is(
    hash <- store_location_group(
      location_group = ut.location_group,
      conn = conn
    ),
    "SQL"
  )
  hash@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.location_group %>%
    select_(~description, ~scheme) %>%
    arrange_(~scheme, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          description, scheme
        FROM
          public.location_group
        ORDER BY
          scheme, description;"
      )
    )

  DBI::dbDisconnect(conn)
})
