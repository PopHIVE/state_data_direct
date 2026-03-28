# ingest.R - Washington (WA) Respiratory Surveillance
# Provider: Washington State Department of Health
# Tier 1 | Strategy: direct_csv (multi-file)
# Run from data/wa_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "53"
state_name  <- "Washington"
source_urls <- c("https://doh.wa.gov/data-and-statistical-reports/diseases-and-chronic-conditions/communicable-disease-surveillance-data/respiratory-illness-data-dashboard")

# Known direct CSV URLs (discovered via dashboard page scraping, confirmed 2026-03)
download_urls <- c(
  "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_CDC_Downloadable_Data.csv",
  "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_WHALES_Downloadable_Data.csv",
  "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_RHINO_Downloadable_Data.csv",
  "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Downloadable_Wastewater.csv",
  "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/wahealth_hospitaluse_download.csv"
)

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

result <- tryCatch({
  resp_keywords <- c("influenza","rsv","covid","respiratory","positive","cases",
                     "surveillance","ili","ari","pcr","virus","percent","hosp","death","flu",
                     "pathogen","season","week","wastewater","concentration")
  all_data   <- list()
  found_urls <- character(0)

  for (dl_url in download_urls) {
    # Skip data dictionary files
    if (grepl("dict|dictionary|methodology|readme", dl_url, ignore.case=TRUE)) next
    fname <- basename(dl_url)
    local_file <- paste0("raw/wa_", length(all_data) + 1, ".csv")
    r <- tryCatch(httr::GET(dl_url, httr::timeout(60),
      httr::write_disk(local_file, overwrite=TRUE),
      httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
    if (is.null(r) || httr::status_code(r) != 200) next
    d <- tryCatch(vroom::vroom(local_file, show_col_types=FALSE), error=function(e) NULL)
    if (is.null(d) || nrow(d) == 0) next
    col_text <- paste(tolower(names(d)), collapse=" ")
    sam_text <- paste(tolower(unlist(head(d, 3))), collapse=" ")
    ok <- any(sapply(resp_keywords, function(k) grepl(k, col_text))) ||
          any(sapply(resp_keywords, function(k) grepl(k, sam_text)))
    if (ok) {
      names(d) <- tolower(gsub("[^a-z0-9]", "_", names(d)))
      d$source_file <- fname
      all_data[[length(all_data) + 1]] <- d
      found_urls <- c(found_urls, dl_url)
    }
  }

  if (length(all_data) == 0) stop("No WA DOH data files downloaded successfully")

  data_raw <- dplyr::bind_rows(all_data)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("WA DOH:", nrow(data_raw), "rows from",
                     length(found_urls), "files:", paste(basename(found_urls), collapse=", ")))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))
