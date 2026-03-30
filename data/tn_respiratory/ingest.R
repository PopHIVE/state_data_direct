# ingest.R - Tennessee (TN) Respiratory Surveillance
# Provider: Tennessee Department of Health
# Tier 2 | Strategy: chromote
# Run from data/tn_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "47"
state_name  <- "Tennessee"
source_urls <- c("https://www.tn.gov/health/ceds-weeklyreports/respiratory-trends.html", "https://www.tn.gov/content/tn/health/cedep/immunization-program/ip/flu-in-tennessee.html")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# Chromote headless browser: Tennessee
# Renders JS dashboard, then discovers: download links, ArcGIS REST, Socrata APIs
result <- tryCatch({
  library(chromote)

  b <- ChromoteSession$new()
  on.exit(tryCatch(b$close(), error=function(e) NULL), add=TRUE)

  all_data   <- list()
  found_urls <- character(0)
  resp_kw <- c("influenza","rsv","covid","respiratory","ili","ari",
               "surveillance","virus","cases","percent","hosp","death","flu","disease",
               "outpatient","positive","emergency","syndromic","season")
  exclude_pat <- "dict|dictionary|data.dict|methodology|readme|template|codebook|glossary|data.definition|immun|vaccine"

  for (page_url in source_urls) {
    tryCatch(b$Page$navigate(page_url, wait_=TRUE, timeout_=25), error=function(e) NULL)
    Sys.sleep(12)

    html_content <- tryCatch(
      b$Runtime$evaluate("document.documentElement.outerHTML")$result$value,
      error=function(e) "")
    if (nchar(html_content) < 200) next

    html_dom <- tryCatch(rvest::read_html(html_content), error=function(e) NULL)

    # Strategy 1: Collect ALL download links after JS renders
    if (!is.null(html_dom)) {
      links <- html_dom |> rvest::html_nodes("a") |> rvest::html_attr("href")
      links <- links[!is.na(links)]
      data_links <- links[grepl("[.](csv|xlsx?|json|tsv)([?#]|$)", links, ignore.case=TRUE)]
      data_links <- data_links[!grepl(exclude_pat, data_links, ignore.case=TRUE)]

      for (dl_url in data_links[seq_len(min(10, length(data_links)))]) {
        if (!grepl("^https?://", dl_url)) {
          bp <- httr::parse_url(page_url)
          dl_url <- if (grepl("^//", dl_url)) paste0(bp$scheme, ":", dl_url)
                    else if (grepl("^/", dl_url)) paste0(bp$scheme, "://", bp$hostname, dl_url)
                    else paste0(page_url, "/", dl_url)
        }
        if (dl_url %in% found_urls) next
        ext <- tolower(sub(".*[.]([a-zA-Z0-9]{1,5})([?#].*)?$", "\\1", dl_url))
        if (!ext %in% c("csv","xlsx","xls","json","tsv")) ext <- "csv"
        local_f <- paste0("raw/tn_dl_", length(all_data) + 1, ".", ext)
        dl_resp <- tryCatch(httr::GET(dl_url, httr::timeout(90),
          httr::write_disk(local_f, overwrite=TRUE),
          httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(dl_resp) || httr::status_code(dl_resp) != 200) next
        d <- if (ext %in% c("xlsx","xls")) {
          tryCatch(as.data.frame(readxl::read_excel(local_f)), error=function(e) NULL)
        } else if (ext == "json") {
          tryCatch(as.data.frame(jsonlite::fromJSON(local_f)), error=function(e) NULL)
        } else {
          tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
        }
        if (!is.null(d) && nrow(d) >= 50) {
          col_text <- paste(tolower(names(d)), collapse=" ")
          sam_text <- paste(tolower(unlist(head(d, 3))), collapse=" ")
          ok <- any(sapply(resp_kw, function(k) grepl(k, col_text))) ||
                any(sapply(resp_kw, function(k) grepl(k, sam_text)))
          if (ok) {
            names(d) <- make.unique(tolower(gsub("[^a-z0-9]", "_", names(d))))
            d$source_page <- basename(page_url)
            all_data[[length(all_data) + 1]] <- d
            found_urls <- c(found_urls, dl_url)
          }
        }
      }
    }

    # Strategy 2: ArcGIS FeatureServer URLs in page source
    fs_matches <- unique(regmatches(html_content,
      gregexpr("https?://[^[:space:]\"']+/FeatureServer/[0-9]+", html_content))[[1]])
    for (fs_url in fs_matches[seq_len(min(5, length(fs_matches)))]) {
      if (fs_url %in% found_urls) next
      q_url <- paste0(fs_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
      r <- tryCatch(httr::GET(q_url, httr::timeout(60), httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
      if (is.null(r) || httr::status_code(r) != 200) next
      jd <- tryCatch(jsonlite::fromJSON(httr::content(r, "text")), error=function(e) NULL)
      if (!is.null(jd$features) && length(jd$features) > 0) {
        d <- tryCatch(as.data.frame(jd$features$attributes), error=function(e) NULL)
        if (!is.null(d) && nrow(d) > 0) {
          names(d) <- make.unique(tolower(gsub("[^a-z0-9]", "_", names(d))))
          d$source_page <- basename(page_url)
          all_data[[length(all_data) + 1]] <- d
          found_urls <- c(found_urls, fs_url)
        }
      }
    }

    # Strategy 3: Socrata resource endpoints embedded in page
    soc_matches <- unique(regmatches(html_content,
      gregexpr("https?://[a-zA-Z0-9.-]+[.]gov/resource/[a-z0-9]{4}-[a-z0-9]{4}", html_content))[[1]])
    for (soc_base in soc_matches[seq_len(min(3, length(soc_matches)))]) {
      if (soc_base %in% found_urls) next
      csv_url <- paste0(soc_base, ".csv?$limit=50000")
      local_f <- paste0("raw/tn_socrata_", length(all_data) + 1, ".csv")
      r <- tryCatch(httr::GET(csv_url, httr::timeout(60),
        httr::write_disk(local_f, overwrite=TRUE),
        httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
      if (is.null(r) || httr::status_code(r) != 200) next
      d <- tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
      if (!is.null(d) && nrow(d) > 0) {
        names(d) <- make.unique(tolower(gsub("[^a-z0-9]", "_", names(d))))
        d$source_page <- basename(page_url)
        all_data[[length(all_data) + 1]] <- d
        found_urls <- c(found_urls, soc_base)
      }
    }
  }

  if (length(all_data) == 0) stop("Chromote: no accessible respiratory data found on TN pages")

  # Cast all columns to character before binding (mixed types from Excel files)
  all_data <- lapply(all_data, function(df) dplyr::mutate(df, dplyr::across(dplyr::everything(), as.character)))
  data_raw <- dplyr::bind_rows(all_data)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("TN:", nrow(data_raw), "rows from",
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

