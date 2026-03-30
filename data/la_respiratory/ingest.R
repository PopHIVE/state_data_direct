# ingest.R - Louisiana (LA) Respiratory Surveillance
# Provider: Louisiana Department of Health
# Tier 1 | Strategy: direct_csv + tableau
# Run from data/la_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "22"
state_name  <- "Louisiana"
source_urls <- c(
  "https://ldh.la.gov/page/respiratory-hospitalization-data",
  "https://ldh.la.gov/page/respiratory-emergency-department-visits-data",
  "https://ldh.la.gov/page/respiratory-laboratory-survey-data"
)

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

result <- tryCatch({
  all_data   <- list()
  found_urls <- character(0)

  # Strategy 1: Direct LDH Excel downloads (COVID cases/testing data)
  direct_urls <- c(
    "https://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_AGE_GENDER_BYWEEK_PUBLICUSE.xlsx",
    "https://ldh.la.gov/assets/oph/Coronavirus/data/LA_COVID_TESTBYDAY_PARISH_PUBLICUSE.xlsx"
  )
  for (dl_url in direct_urls) {
    fname <- basename(dl_url)
    local_f <- paste0("raw/la_", length(all_data) + 1, ".xlsx")
    r <- tryCatch(httr::GET(dl_url, httr::timeout(120),
      httr::write_disk(local_f, overwrite = TRUE),
      httr::user_agent("Mozilla/5.0")), error = function(e) NULL)
    if (is.null(r) || httr::status_code(r) != 200) next
    d <- tryCatch(as.data.frame(readxl::read_excel(local_f)),
      error = function(e) NULL)
    if (!is.null(d) && nrow(d) >= 10) {
      names(d) <- make.unique(tolower(gsub("[^a-z0-9]+", "_", names(d))))
      d$source_file <- fname
      all_data[[length(all_data) + 1]] <- d
      found_urls <- c(found_urls, fname)
    }
  }

  # Strategy 2: Tableau CSV downloads from embedded dashboards
  tableau_views <- c(
    "https://analytics.la.gov/t/LDH/views/QAVERSION-LouisianaCovidDashboard/MainDashboard.csv?:showVizHome=no",
    "https://analytics.la.gov/t/LDH/views/QAVERSION-extracovidinfo/Dashboard1.csv?:showVizHome=no"
  )
  for (tab_url in tableau_views) {
    view_name <- sub(".*/([^/]+)\\.csv.*", "\\1", tab_url)
    local_f <- paste0("raw/la_tableau_", tolower(view_name), ".csv")
    r <- tryCatch(httr::GET(tab_url, httr::timeout(60),
      httr::user_agent("Mozilla/5.0"),
      httr::write_disk(local_f, overwrite = TRUE)),
      error = function(e) NULL)
    if (is.null(r) || httr::status_code(r) != 200) next
    d <- tryCatch(vroom::vroom(local_f, show_col_types = FALSE),
      error = function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) {
      names(d) <- make.unique(tolower(gsub("[^a-z0-9]+", "_", names(d))))
      d$source_file <- paste0("Tableau_", view_name)
      all_data[[length(all_data) + 1]] <- d
      found_urls <- c(found_urls, paste0("Tableau/", view_name))
    }
  }

  # Strategy 3: Scrape LDH pages for any additional data links
  resp_kw <- c("influenza", "rsv", "covid", "respiratory", "hosp",
               "death", "flu", "disease", "emergency", "positiv")
  for (page_url in source_urls) {
    resp <- tryCatch(httr::GET(page_url, httr::timeout(30),
      httr::user_agent("Mozilla/5.0")), error = function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next
    html <- tryCatch(rvest::read_html(httr::content(resp, "text",
      encoding = "UTF-8")), error = function(e) NULL)
    if (is.null(html)) next
    links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
    links <- links[!is.na(links)]
    data_links <- links[grepl("[.](csv|xlsx?|json|tsv)([?#]|$)",
      links, ignore.case = TRUE)]
    # Skip files we already downloaded directly
    data_links <- data_links[!grepl("TESTBYDAY|AGE_GENDER|Vaccine",
      data_links, ignore.case = TRUE)]
    bp <- httr::parse_url(page_url)
    for (dl_url in unique(data_links)[seq_len(min(5,
      length(unique(data_links))))]) {
      if (!grepl("^https?://", dl_url)) {
        dl_url <- paste0(bp$scheme, "://", bp$hostname, dl_url)
      }
      if (dl_url %in% found_urls) next
      ext <- tolower(sub(".*[.]([a-z0-9]{1,5})([?#].*)?$", "\\1", dl_url))
      if (!ext %in% c("csv", "xlsx", "xls")) ext <- "xlsx"
      local_f <- paste0("raw/la_page_", length(all_data) + 1, ".", ext)
      r <- tryCatch(httr::GET(dl_url, httr::timeout(90),
        httr::write_disk(local_f, overwrite = TRUE),
        httr::user_agent("Mozilla/5.0")), error = function(e) NULL)
      if (is.null(r) || httr::status_code(r) != 200) next
      d <- if (ext %in% c("xlsx", "xls")) {
        tryCatch(as.data.frame(readxl::read_excel(local_f)),
          error = function(e) NULL)
      } else {
        tryCatch(vroom::vroom(local_f, show_col_types = FALSE),
          error = function(e) NULL)
      }
      if (!is.null(d) && nrow(d) >= 10) {
        names(d) <- make.unique(tolower(gsub("[^a-z0-9]+", "_", names(d))))
        d$source_file <- basename(dl_url)
        all_data[[length(all_data) + 1]] <- d
        found_urls <- c(found_urls, dl_url)
      }
    }
  }

  if (length(all_data) == 0) stop("No LA respiratory data downloaded")

  all_data <- lapply(all_data, function(df) {
    dplyr::mutate(df, dplyr::across(dplyr::everything(), as.character))
  })
  data_raw <- dplyr::bind_rows(all_data)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim = ",")
  process$success <- TRUE

  list(success = TRUE, rows = nrow(data_raw),
       message = paste("LA:", nrow(data_raw), "rows from",
         length(found_urls), "sources:",
         paste(found_urls, collapse = ", ")))

}, error = function(e) {
  list(success = FALSE, rows = 0L, message = conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox = TRUE, pretty = TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))
