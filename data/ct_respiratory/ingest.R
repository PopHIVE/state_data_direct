# ingest.R - Connecticut (CT) Respiratory Surveillance
# Provider: Connecticut Department of Public Health
# Tier 1 | Strategy: socrata
# Run from data/ct_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "09"
state_name  <- "Connecticut"
source_urls <- c("https://data.ct.gov/resource/8d4q-hwjx.csv")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# Socrata open data portal: data.ct.gov
result <- tryCatch({
  socrata_id   <- "8d4q-hwjx"
  socrata_host <- "data.ct.gov"

  # Try CSV endpoint first (more reliable for bulk data)
  csv_url <- sprintf("https://%s/resource/%s.csv?$limit=50000", socrata_host, socrata_id)
  resp <- httr::GET(csv_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"))

  if (httr::status_code(resp) == 200) {
    writeBin(httr::content(resp, "raw"), "raw/data.csv")
    data_raw <- vroom::vroom("raw/data.csv", show_col_types=FALSE)
  } else {
    # Fallback to JSON endpoint
    json_url <- sprintf("https://%s/resource/%s.json?$limit=50000", socrata_host, socrata_id)
    resp <- httr::GET(json_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"))
    if (httr::status_code(resp) != 200) stop(paste("Socrata API returned HTTP", httr::status_code(resp)))
    data_raw <- as.data.frame(jsonlite::fromJSON(httr::content(resp, "text")))
  }

  if (nrow(data_raw) == 0) stop("Socrata API returned 0 rows")

  names(data_raw) <- tolower(gsub("[^a-z0-9]", "_", names(data_raw)))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Socrata data:", nrow(data_raw), "rows from", socrata_id))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

