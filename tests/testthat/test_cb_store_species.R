context("store_species")
conn <- connect_db()
ut <- sprintf("unit test %i", 1:2)
ut.species_group <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "fingerprint"],
  stringsAsFactors = FALSE
)
ut.species_group2 <- data.frame(
  local_id = ut,
  description = ut,
  scheme = DBI::dbReadTable(conn, "scheme")[1, "fingerprint"],
  stringsAsFactors = TRUE
)
DBI::dbDisconnect(conn)

test_that("store_species_group works correctly", {
  conn <- connect_db()

  expect_is(
    sg <- store_species_group(
      species_group = ut.species_group,
      conn = conn
    ),
    "SQL"
  )
  sg@.Data %>%
    gsub(pattern = "\\\"", replacement = "") %>%
    c("staging") %>%
    rev() %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_false()
  ut.species_group %>%
    select_(~description, ~scheme) %>%
    arrange_(~scheme, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          sg.description,
          s.fingerprint AS scheme
        FROM
          public.species_group AS sg
        INNER JOIN
          public.scheme AS s
        ON
          sg.scheme = s.id
        ORDER BY
          s.fingerprint, sg.description;"
      )
    )

  expect_is(
    sg <- store_species_group(
      species_group = ut.species_group2,
      hash = "junk",
      clean = FALSE,
      conn = conn
    ),
    "SQL"
  )
  c("staging", "species_group_junk") %>%
    DBI::dbExistsTable(conn = conn) %>%
    expect_true()
  c("staging", "species_group_junk") %>%
    DBI::dbRemoveTable(conn = conn) %>%
    expect_true()
  ut.species_group2 %>%
    select_(~description, ~scheme) %>%
    mutate_each_(funs(as.character), vars = c("scheme", "description")) %>%
    arrange_(~scheme, ~description) %>%
    expect_identical(
      dbGetQuery(
        conn,
        "SELECT
          sg.description,
          s.fingerprint AS scheme
        FROM
          public.species_group AS sg
        INNER JOIN
          public.scheme AS s
        ON
          sg.scheme = s.id
        ORDER BY
          s.fingerprint, sg.description;"
      )
    )

  DBI::dbDisconnect(conn)
})
