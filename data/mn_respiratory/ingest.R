# ingest.R - Minnesota (MN) Respiratory Surveillance
# Provider: Minnesota Department of Health
# Tier 2 | Strategy: html (multi-page, collect all CSVs)
# Run from data/mn_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "27"
state_name  <- "Minnesota"
source_urls <- c("https://www.health.state.mn.us/diseases/respiratory/stats/lab.html",
                 "https://www.health.state.mn.us/diseases/respiratory/stats/hosp.html",
                 "https://www.health.state.mn.us/diseases/flu/stats/out.html",
                 "https://www.health.state.mn.us/diseases/respiratory/stats/setting.html",
                 "https://www.health.state.mn.us/diseases/respiratory/stats/tsys.html")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

result <- tryCatch({
  resp_keywords <- c("influenza","rsv","covid","respiratory","positive","cases",
                     "surveillance","ili","ari","pcr","antigen","virus","percent",
                     "hosp","death","flu","pathogen","season","week","mmwr")
  all_data  <- list()
  found_urls <- character(0)

  for (page_url in source_urls) {
    resp <- tryCatch(httr::GET(page_url, httr::timeout(30),
      httr::user_agent("Mozilla/5.0 (compatible; R scraper)")),
      error = function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next

    html  <- rvest::read_html(httr::content(resp, "text", encoding="UTF-8"))
    links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
    links <- links[!is.na(links)]

    # Find direct data file links
    data_links <- links[grepl("[.](csv|xlsx?|json|tsv|xls)([?#]|$)", links, ignore.case=TRUE)]
    # Exclude metadata/reference files
    data_links <- data_links[!grepl("dict|dictionary|methodology|readme|template|codebook",
                                    data_links, ignore.case=TRUE)]

    # Resolve relative URLs
    base <- httr::parse_url(page_url)
    data_links <- sapply(data_links, function(l) {
      if (grepl("^https?://", l)) l
      else if (grepl("^//", l)) paste0(base$scheme, ":", l)
      else if (grepl("^/", l)) paste0(base$scheme, "://", base$hostname, l)
      else paste0(dirname(page_url), "/", l)
    })

    # Download and validate each data file from this page
    for (dl_url in unique(data_links)[seq_len(min(10, length(unique(data_links))))]) {
      if (dl_url %in% found_urls) next  # skip already downloaded
      url_path <- tryCatch(httr::parse_url(dl_url)$path, error=function(e) dl_url)
      ext <- tolower(sub(".*[.]([a-zA-Z0-9]{1,5})$", "\\1", url_path))
      if (!ext %in% c("csv","xlsx","xls","json","tsv")) ext <- "csv"
      local_file <- paste0("raw/mn_", length(all_data) + 1, ".", ext)
      dl_resp <- tryCatch(httr::GET(dl_url, httr::timeout(60),
        httr::write_disk(local_file, overwrite=TRUE),
        httr::user_agent("Mozilla/5.0 (compatible; R scraper)")), error=function(e) NULL)
      if (is.null(dl_resp) || httr::status_code(dl_resp) != 200) next
      d <- if (ext %in% c("xlsx","xls")) {
        tryCatch(as.data.frame(readxl::read_excel(local_file)), error=function(e) NULL)
      } else if (ext == "json") {
        tryCatch(as.data.frame(jsonlite::fromJSON(local_file)), error=function(e) NULL)
      } else {
        tryCatch(vroom::vroom(local_file, show_col_types=FALSE), error=function(e) NULL)
      }
      if (is.null(d) || nrow(d) == 0) next
      col_text <- paste(tolower(names(d)), collapse=" ")
      sam_text <- paste(tolower(unlist(head(d, 3))), collapse=" ")
      ok <- any(sapply(resp_keywords, function(k) grepl(k, col_text))) ||
            any(sapply(resp_keywords, function(k) grepl(k, sam_text)))
      if (ok) {
        names(d) <- tolower(gsub("[^a-z0-9]", "_", names(d)))
        d$source_page <- basename(page_url)
        all_data[[length(all_data) + 1]] <- d
        found_urls <- c(found_urls, dl_url)
      }
    }
  }

  if (length(all_data) == 0) stop("No CSV/Excel/JSON download links found on any source page")

  # Combine all data frames (allow different columns — fill with NA)
  data_raw <- dplyr::bind_rows(all_data)

  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Downloaded", nrow(data_raw), "rows from",
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
