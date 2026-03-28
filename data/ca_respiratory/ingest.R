# ingest.R - California (CA) Respiratory Surveillance
# Provider: California Department of Public Health
# Tier 1 | Strategy: ckan
# Run from data/ca_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "06"
state_name  <- "California"
source_urls <- c("https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/RespiratoryVirusReport.aspx")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# CKAN open data portal: https://data.chhs.ca.gov
result <- tryCatch({
  ckan_base    <- "https://data.chhs.ca.gov"
  ckan_dataset <- "respiratory-virus-dashboard"

  # Query CKAN package API to find CSV resource
  pkg_url  <- paste0(ckan_base, "/api/3/action/package_show?id=", ckan_dataset)
  pkg_resp <- httr::GET(pkg_url, httr::timeout(30), httr::user_agent("Mozilla/5.0"))
  if (httr::status_code(pkg_resp) != 200) stop(paste("CKAN API returned HTTP", httr::status_code(pkg_resp)))

  pkg_data  <- jsonlite::fromJSON(httr::content(pkg_resp, "text"))
  resources <- pkg_data$result$resources

  # Find CSV resource
  csv_resources <- resources[grepl("csv", tolower(resources$format)), ]
  if (nrow(csv_resources) == 0) {
    csv_resources <- resources[grepl("[.]csv", tolower(resources$url)), ]
  }
  if (nrow(csv_resources) == 0) stop("No CSV resource found in CKAN dataset")

  csv_url <- csv_resources$url[1]
  dl_resp <- httr::GET(csv_url, httr::timeout(120),
    httr::write_disk("raw/data.csv", overwrite=TRUE),
    httr::user_agent("Mozilla/5.0"))
  if (httr::status_code(dl_resp) != 200) stop(paste("CSV download failed:", httr::status_code(dl_resp)))

  data_raw <- vroom::vroom("raw/data.csv", show_col_types=FALSE)
  if (nrow(data_raw) == 0) stop("Downloaded CSV has 0 rows")

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("CKAN data:", nrow(data_raw), "rows from", ckan_dataset))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

