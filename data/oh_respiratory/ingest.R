# ingest.R - Ohio (OH) Respiratory Surveillance
# Provider: Ohio Department of Health
# Tier 1 | Strategy: oh_special
# Run from data/oh_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "39"
state_name  <- "Ohio"
source_urls <- c("https://data.ohio.gov/wps/portal/gov/data/view/ohio-department-of-health-respiratory-dashboard")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# Ohio - data.ohio.gov open data portal (Socrata-based)
result <- tryCatch({
  # Try to find Socrata dataset ID from the portal page
  portal_url <- source_urls[1]
  resp <- httr::GET(portal_url, httr::timeout(30), httr::user_agent("Mozilla/5.0"))

  # data.ohio.gov uses a custom portal; try known dataset patterns
  # Search for JSON API endpoint via page scraping
  page_text <- httr::content(resp, "text", encoding="UTF-8")

  # Extract dataset IDs (Socrata format: 4-char-4-char)
  id_matches <- regmatches(page_text, gregexpr("[a-z0-9]{4}-[a-z0-9]{4}", page_text))[[1]]
  id_matches <- unique(id_matches)

  # Also try the OAKS (Ohio portal) API endpoint pattern
  # Try a direct search via data.ohio.gov catalog API
  search_resp <- tryCatch(
    httr::GET("https://data.ohio.gov/api/catalog/v1?search=respiratory+health&limit=10",
              httr::timeout(30)),
    error=function(e) NULL)

  found_data <- FALSE
  data_raw   <- NULL

  if (!is.null(search_resp) && httr::status_code(search_resp) == 200) {
    catalog <- tryCatch(jsonlite::fromJSON(httr::content(search_resp, "text")), error=function(e) NULL)
    if (!is.null(catalog$results) && length(catalog$results) > 0) {
      # Try first matching dataset
      ds_id <- catalog$results$id[1]
      api_url <- paste0("https://data.ohio.gov/resource/", ds_id, ".json?$limit=50000")
      data_resp <- httr::GET(api_url, httr::timeout(60))
      if (httr::status_code(data_resp) == 200) {
        data_raw <- as.data.frame(jsonlite::fromJSON(httr::content(data_resp, "text")))
        found_data <- TRUE
      }
    }
  }

  # Try CSV download directly
  if (!found_data) {
    # Try common Ohio respiratory dataset ID patterns
    candidate_ids <- c("4zti-3ab3", "qtaz-5kaw", "rdvy-kkni")
    for (ds_id in candidate_ids) {
      api_url  <- paste0("https://data.ohio.gov/resource/", ds_id, ".csv?$limit=50000")
      dl_resp  <- tryCatch(httr::GET(api_url, httr::timeout(30),
                    httr::write_disk("raw/data.csv", overwrite=TRUE)), error=function(e) NULL)
      if (!is.null(dl_resp) && httr::status_code(dl_resp) == 200) {
        d <- tryCatch(vroom::vroom("raw/data.csv", show_col_types=FALSE), error=function(e) NULL)
        if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; break }
      }
    }
  }

  if (!found_data) stop("Ohio data.ohio.gov dataset not accessible via API; dataset ID unknown")

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Ohio data retrieved:", nrow(data_raw), "rows"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

