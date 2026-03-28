# ingest.R - North Carolina (NC) Respiratory Surveillance
# Provider: North Carolina Department of Health and Human Services
# Tier 1 | Strategy: nc_special
# Run from data/nc_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "37"
state_name  <- "North Carolina"
source_urls <- c("https://covid19.ncdhhs.gov/dashboard/data-behind-dashboards")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# North Carolina - "data behind dashboards" page with direct CSV downloads
result <- tryCatch({
  url  <- source_urls[1]
  resp <- httr::GET(url, httr::timeout(30), httr::user_agent("Mozilla/5.0"))
  if (httr::status_code(resp) != 200) stop(paste("HTTP", httr::status_code(resp)))

  html  <- rvest::read_html(httr::content(resp, "text", encoding="UTF-8"))
  links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
  links <- links[!is.na(links)]

  # Find CSV/Excel data file links only (skip PDFs)
  data_links <- links[grepl("[.](csv|xlsx?|tsv|json)([?]|$)", links, ignore.case=TRUE)]

  # Also look for links containing "download" or "data" (but not PDFs)
  if (length(data_links) == 0) {
    candidate <- links[grepl("download|/data/|data[.]csv|export", links, ignore.case=TRUE)]
    data_links <- candidate[!grepl("[.]pdf([?]|$)", candidate, ignore.case=TRUE)]
  }

  if (length(data_links) == 0) {
    pdf_links <- links[grepl("[.]pdf([?]|$)", links, ignore.case=TRUE)]
    if (length(pdf_links) > 0) stop(paste("Page only has PDF downloads, not machine-readable data:", pdf_links[1]))
    stop("No downloadable data files found on NC data-behind-dashboards page")
  }

  # Download all found files
  all_data <- list()
  for (i in seq_along(data_links)) {
    dl_url <- data_links[i]
    if (!grepl("^https?://", dl_url)) {
      base <- httr::parse_url(url)
      dl_url <- if (grepl("^/", dl_url)) paste0(base$scheme, "://", base$hostname, dl_url)
                else paste0(url, "/", dl_url)
    }
    local_f <- paste0("raw/nc_data_", i, ".csv")
    dl_resp <- tryCatch(httr::GET(dl_url, httr::timeout(60), httr::write_disk(local_f, overwrite=TRUE)),
                        error=function(e) NULL)
    if (!is.null(dl_resp) && httr::status_code(dl_resp) == 200) {
      d <- tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
      if (!is.null(d)) all_data[[length(all_data)+1]] <- d
    }
  }

  if (length(all_data) == 0) stop("Failed to download any NC data files")

  # Standardize columns
  data_raw <- all_data[[1]]
  names(data_raw) <- tolower(gsub("[^a-z0-9]", "_", names(data_raw)))

  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("NC data downloaded:", nrow(data_raw), "rows from", length(all_data), "files"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

