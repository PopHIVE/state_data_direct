# ingest.R - Oregon (OR) Respiratory Surveillance
# Provider: Oregon Health Authority
# Tier 1 | Strategy: tableau_csv + html
# Run from data/or_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "41"
state_name  <- "Oregon"
source_urls <- c("https://www.oregon.gov/oha/ph/diseasesconditions/communicabledisease/diseasesurveillancedata/influenza/pages/surveil.aspx",
                 "https://www.oregon.gov/oha/ph/diseasesconditions/communicabledisease/diseasesurveillancedata/pages/respiratorysyncytialvirussurveillancedata.aspx")

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

  # Strategy 1: Tableau Public CSV downloads (primary source for OR respiratory)
  # These views contain COVID, Influenza, RSV deaths/test positivity/outbreaks
  tableau_views <- c(
    "https://public.tableau.com/views/OregonsRespiratoryVirusData/Deaths.csv?:showVizHome=no",
    "https://public.tableau.com/views/OregonsRespiratoryVirusData/TestPositivity.csv?:showVizHome=no",
    "https://public.tableau.com/views/OregonsRespiratoryVirusData/Outbreaks.csv?:showVizHome=no",
    "https://public.tableau.com/views/OregonsRespiratoryVirusData/Hospitalizations.csv?:showVizHome=no",
    "https://public.tableau.com/views/OregonsRespiratoryVirusData/Overview.csv?:showVizHome=no"
  )
  for (tab_url in tableau_views) {
    view_name <- sub(".*/([^/]+)\\.csv.*", "\\1", tab_url)
    local_f <- paste0("raw/or_tableau_", tolower(view_name), ".csv")
    r <- tryCatch(httr::GET(tab_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"),
      httr::write_disk(local_f, overwrite=TRUE)), error=function(e) NULL)
    if (is.null(r) || httr::status_code(r) != 200) next
    d <- tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) {
      names(d) <- make.unique(tolower(gsub("[^a-z0-9]", "_", names(d))))
      d$source_dataset <- paste0("tableau_", view_name)
      all_data[[length(all_data) + 1]] <- d
      found_urls <- c(found_urls, paste0("Tableau/", view_name))
    }
  }

  # Strategy 2: HTML scraping of OHA surveillance pages for CSV/Excel links
  for (page_url in source_urls) {
    resp <- tryCatch(httr::GET(page_url, httr::timeout(30), httr::user_agent("Mozilla/5.0")),
      error=function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next
    html <- tryCatch(rvest::read_html(httr::content(resp, "text", encoding="UTF-8")), error=function(e) NULL)
    if (is.null(html)) next
    links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
    links <- links[!is.na(links)]
    data_links <- links[grepl("[.](csv|xlsx?|json|tsv)([?#]|$)", links, ignore.case=TRUE)]
    data_links <- data_links[!grepl("dict|dictionary|methodology", data_links, ignore.case=TRUE)]
    base <- httr::parse_url(page_url)
    data_links <- sapply(data_links, function(l) {
      if (grepl("^https?://", l)) l
      else if (grepl("^/", l)) paste0(base$scheme, "://", base$hostname, l)
      else paste0(dirname(page_url), "/", l)
    })
    for (dl_url in unique(data_links)[seq_len(min(5, length(unique(data_links))))]) {
      if (dl_url %in% found_urls) next
      ext <- tolower(sub(".*[.]([a-z0-9]{1,5})([?#].*)?$", "\\1", dl_url))
      if (!ext %in% c("csv","xlsx","xls","json","tsv")) ext <- "csv"
      local_f <- paste0("raw/or_html_", length(all_data)+1, ".", ext)
      r2 <- tryCatch(httr::GET(dl_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"),
        httr::write_disk(local_f, overwrite=TRUE)), error=function(e) NULL)
      if (is.null(r2) || httr::status_code(r2) != 200) next
      d <- if (ext %in% c("xlsx","xls")) {
        tryCatch(as.data.frame(readxl::read_excel(local_f)), error=function(e) NULL)
      } else {
        tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
      }
      if (!is.null(d) && nrow(d) > 0) {
        names(d) <- make.unique(tolower(gsub("[^a-z0-9]", "_", names(d))))
        d$source_dataset <- basename(dl_url)
        all_data[[length(all_data) + 1]] <- d
        found_urls <- c(found_urls, dl_url)
      }
    }
  }

  if (length(all_data) == 0) stop("No Oregon data downloaded from any source")

  data_raw <- dplyr::bind_rows(all_data)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Oregon:", nrow(data_raw), "rows from",
                     length(found_urls), "sources:", paste(found_urls, collapse=", ")))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))
