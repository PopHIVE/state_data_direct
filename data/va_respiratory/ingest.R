# ingest.R - Virginia (VA) Respiratory Surveillance
# Provider: Virginia Department of Health
# Tier 1 | Strategy: direct_csv
# Run from data/va_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "51"
state_name  <- "Virginia"
source_urls <- c("https://www.vdh.virginia.gov/epidemiology/respiratory-diseases-in-virginia/data/")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# Direct CSV download: Virginia (VDH Public Use Datasets — confirmed 2026-03)
result <- tryCatch({
  dl_urls <- c(
    "https://data.virginia.gov/dataset/31f16f23-122a-40fe-b5a3-96325c7eb6e9/resource/a0da176d-3e45-427f-a6fd-f1bcb8a71136/download/t-pud-respiratory-illness-activity.csv",
    "https://data.virginia.gov/dataset/15418583-c9b0-40a4-98dc-209f21ecb781/resource/b22fc729-830c-4a29-b088-9da5dd47be6f/download/t-pud-respiratory-emergency-department-visits.csv",
    "https://data.virginia.gov/dataset/e602390a-f4f8-46bd-a4aa-14157b7f4fde/resource/f4912847-9f9c-420e-84dd-c39df614acc0/download/t-pud-respiratory-death-data.csv",
    "https://data.virginia.gov/dataset/bbe88af0-d566-47bd-87dd-de70d164cf10/resource/bd3956ed-fbf4-4826-b6b5-c3bbe338b8d8/download/t-pud-respiratory-influenza-associated-pediatric-death.csv",
    "https://data.virginia.gov/dataset/eabea704-27d3-4d60-9078-5e1084f74db1/resource/8f013501-a5bd-45f0-9b2c-5aa09f67a04e/download/t-pud-influenza-la.csv",
    "https://data.virginia.gov/dataset/b98515e8-2004-4af1-9860-d0dab94452b4/resource/1d3d1839-5c83-4397-896a-c397926b56c4/download/vdh-t-pud-covid-lab.csv",
    "https://data.virginia.gov/dataset/cf92b99f-0dc6-496d-99a7-3be164ff78fc/resource/2855e63f-e565-4619-b003-c49141d14fd1/download/t-pud-respiratory-outbreaks.csv"
  )
  all_data <- list()

  for (i in seq_along(dl_urls)) {
    local_f <- paste0("raw/download_", i, ".csv")
    dl_resp <- httr::GET(dl_urls[i], httr::timeout(120),
      httr::write_disk(local_f, overwrite=TRUE),
      httr::user_agent("Mozilla/5.0 (compatible; R scraper)"))
    if (httr::status_code(dl_resp) == 200) {
      d <- tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
      if (!is.null(d) && nrow(d) > 0) all_data[[length(all_data)+1]] <- d
    }
  }

  if (length(all_data) == 0) stop("Failed to download any CSV files")

  # Combine all downloaded data (coerce to character if needed)
  if (length(all_data) > 1) {
    all_data <- lapply(all_data, function(d) dplyr::mutate(d, dplyr::across(dplyr::everything(), as.character)))
    data_raw <- dplyr::bind_rows(all_data)
  } else {
    data_raw <- all_data[[1]]
  }

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("VDH Direct CSV:", nrow(data_raw), "rows from", length(all_data), "files"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

