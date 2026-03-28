# ingest.R - New York (NY) Respiratory Surveillance
# Provider: New York State Department of Health
# Tier 1 | Strategy: socrata (multi-dataset)
# Run from data/ny_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "36"
state_name  <- "New York"
source_urls <- c("https://health.data.ny.gov/resource/w6ed-sctw.json",
                 "https://coronavirus.health.ny.gov/positive-tests-over-time-region-and-county",
                 "https://coronavirus.health.ny.gov/covid-19-emergency-department-syndromic-surveillance",
                 "https://coronavirus.health.ny.gov/daily-hospitalization-summary",
                 "https://coronavirus.health.ny.gov/hospital-bed-capacity",
                 "https://coronavirus.health.ny.gov/fatalities-0",
                 "https://nyshc.health.ny.gov/web/nyapd/new-york-state-flu-tracker")

# Known Socrata dataset IDs on health.data.ny.gov (confirmed 2026-03)
socrata_datasets <- list(
  list(id="w6ed-sctw",  desc="Nursing Home COVID"),
  list(id="jvfi-ffup",  desc="Statewide COVID-19 Testing"),
  list(id="iye6-rifr",  desc="Influenza Hospitalizations"),
  list(id="jr8b-6gh6",  desc="Influenza Lab Cases by County"),
  list(id="cpxv-79jk",  desc="Influenza Lab Cases by Age Group")
)

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

result <- tryCatch({
  socrata_host <- "health.data.ny.gov"
  all_data <- list()
  found_ids <- character(0)

  for (ds in socrata_datasets) {
    csv_url <- sprintf("https://%s/resource/%s.csv?$limit=200000", socrata_host, ds$id)
    local_f  <- paste0("raw/ny_", ds$id, ".csv")
    resp <- tryCatch(httr::GET(csv_url, httr::timeout(120), httr::user_agent("Mozilla/5.0"),
      httr::write_disk(local_f, overwrite=TRUE)), error=function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next
    d <- tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
    if (is.null(d) || nrow(d) == 0 || ncol(d) == 0) next
    names(d) <- tolower(gsub("[^a-z0-9]", "_", names(d)))
    d$source_dataset <- ds$id
    d$source_desc    <- ds$desc
    all_data[[length(all_data) + 1]] <- d
    found_ids <- c(found_ids, ds$id)
  }

  if (length(all_data) == 0) stop("Socrata API returned 0 rows from all datasets")

  data_raw <- dplyr::bind_rows(all_data)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Socrata data:", nrow(data_raw), "rows from",
                     length(found_ids), "datasets:", paste(found_ids, collapse=", ")))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))
