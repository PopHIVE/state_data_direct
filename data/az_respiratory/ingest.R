# ingest.R - Arizona (AZ) Respiratory Surveillance
# Provider: Arizona Department of Health Services
# Tier 2 | Strategy: chromote
# Run from data/az_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "04"
state_name  <- "Arizona"
source_urls <- c("https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/respiratory-illness/dashboards/index.php", "https://experience.arcgis.com/experience/8c3b1a0dc26448ccb9e1efb3b17e3a00")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# Chromote headless browser: Arizona
# Renders JS dashboard, then discovers: download links, ArcGIS REST, Socrata APIs
result <- tryCatch({
  library(chromote)

  b <- ChromoteSession$new()
  on.exit(tryCatch(b$close(), error=function(e) NULL), add=TRUE)

  data_raw   <- NULL
  found_data <- FALSE
  found_url  <- NULL
  resp_kw <- c("influenza","rsv","covid","respiratory","ili","ari",
               "surveillance","virus","cases","percent","hosp","death","flu","disease")

  for (page_url in source_urls) {
    tryCatch(b$Page$navigate(page_url, wait_=TRUE, timeout_=25), error=function(e) NULL)
    Sys.sleep(10)

    html_content <- tryCatch(
      b$Runtime$evaluate("document.documentElement.outerHTML")$result$value,
      error=function(e) "")
    if (nchar(html_content) < 200) next

    html_dom <- tryCatch(rvest::read_html(html_content), error=function(e) NULL)

    # Strategy 1: Download links visible after JS renders
    if (!is.null(html_dom)) {
      links <- html_dom |> rvest::html_nodes("a") |> rvest::html_attr("href")
      links <- links[!is.na(links)]
      data_links <- links[grepl("[.](csv|xlsx?|json|tsv)([?#]|$)", links, ignore.case=TRUE)]

      for (dl_url in data_links[seq_len(min(8, length(data_links)))]) {
        if (!grepl("^https?://", dl_url)) {
          bp <- httr::parse_url(page_url)
          dl_url <- if (grepl("^//", dl_url)) paste0(bp$scheme, ":", dl_url)
                    else if (grepl("^/", dl_url)) paste0(bp$scheme, "://", bp$hostname, dl_url)
                    else paste0(page_url, "/", dl_url)
        }
        ext <- tolower(sub(".*[.]([a-zA-Z0-9]{1,5})([?#].*)?$", "\\1", dl_url))
        if (!ext %in% c("csv","xlsx","xls","json","tsv")) ext <- "csv"
        local_f <- paste0("raw/chromote_dl.", ext)
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
        if (!is.null(d) && nrow(d) > 0) {
          col_text <- paste(tolower(names(d)), collapse=" ")
          sam_text <- paste(tolower(unlist(head(d, 3))), collapse=" ")
          ok <- any(sapply(resp_kw, function(k) grepl(k, col_text))) ||
                any(sapply(resp_kw, function(k) grepl(k, sam_text)))
          if (ok) { data_raw <- d; found_data <- TRUE; found_url <- dl_url; break }
        }
      }
      if (found_data) break
    }

    # Strategy 2: ArcGIS FeatureServer URLs in page source
    if (!found_data) {
      fs_matches <- unique(regmatches(html_content,
        gregexpr("https?://[^[:space:]]+/FeatureServer/[0-9]+", html_content))[[1]])
      for (fs_url in fs_matches[seq_len(min(5, length(fs_matches)))]) {
        q_url <- paste0(fs_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
        r <- tryCatch(httr::GET(q_url, httr::timeout(60), httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(r) || httr::status_code(r) != 200) next
        jd <- tryCatch(jsonlite::fromJSON(httr::content(r, "text")), error=function(e) NULL)
        if (!is.null(jd$features) && length(jd$features) > 0) {
          d <- tryCatch(as.data.frame(jd$features$attributes), error=function(e) NULL)
          if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; found_url <- fs_url; break }
        }
      }
      if (found_data) break
    }

    # Strategy 3: ArcGIS MapServer URLs in page source
    if (!found_data) {
      ms_matches <- unique(regmatches(html_content,
        gregexpr("https?://[^[:space:]]+/MapServer/[0-9]+", html_content))[[1]])
      for (ms_url in ms_matches[seq_len(min(5, length(ms_matches)))]) {
        q_url <- paste0(ms_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
        r <- tryCatch(httr::GET(q_url, httr::timeout(60), httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(r) || httr::status_code(r) != 200) next
        jd <- tryCatch(jsonlite::fromJSON(httr::content(r, "text")), error=function(e) NULL)
        if (!is.null(jd$features) && length(jd$features) > 0) {
          d <- tryCatch(as.data.frame(jd$features$attributes), error=function(e) NULL)
          if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; found_url <- ms_url; break }
        }
      }
      if (found_data) break
    }

    # Strategy 4: Socrata resource endpoints embedded in page
    if (!found_data) {
      soc_matches <- unique(regmatches(html_content,
        gregexpr("https?://[a-zA-Z0-9.-]+[.]gov/resource/[a-z0-9]{4}-[a-z0-9]{4}", html_content))[[1]])
      for (soc_base in soc_matches[seq_len(min(3, length(soc_matches)))]) {
        csv_url <- paste0(soc_base, ".csv?$limit=50000")
        r <- tryCatch(httr::GET(csv_url, httr::timeout(60),
          httr::write_disk("raw/chromote_socrata.csv", overwrite=TRUE),
          httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(r) || httr::status_code(r) != 200) next
        d <- tryCatch(vroom::vroom("raw/chromote_socrata.csv", show_col_types=FALSE), error=function(e) NULL)
        if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; found_url <- csv_url; break }
      }
      if (found_data) break
    }

    # Strategy 5: Direct CSV/JSON links in page <script> src or iframe src
    if (!found_data && !is.null(html_dom)) {
      script_srcs <- html_dom |> rvest::html_nodes("script,iframe") |> rvest::html_attr("src")
      script_srcs <- script_srcs[!is.na(script_srcs) & grepl("[.](csv|json)([?]|$)", script_srcs)]
      for (s_url in script_srcs[seq_len(min(3, length(script_srcs)))]) {
        if (!grepl("^https?://", s_url)) {
          bp <- httr::parse_url(page_url)
          s_url <- paste0(bp$scheme, "://", bp$hostname, s_url)
        }
        ext <- if (grepl("[.]json", s_url)) "json" else "csv"
        local_f <- paste0("raw/chromote_src.", ext)
        r <- tryCatch(httr::GET(s_url, httr::timeout(60),
          httr::write_disk(local_f, overwrite=TRUE)), error=function(e) NULL)
        if (is.null(r) || httr::status_code(r) != 200) next
        d <- if (ext == "json") {
          tryCatch(as.data.frame(jsonlite::fromJSON(local_f)), error=function(e) NULL)
        } else {
          tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
        }
        if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; found_url <- s_url; break }
      }
      if (found_data) break
    }
  }

  if (!found_data) stop("Chromote: no accessible data found in rendered JS dashboard")

  names(data_raw) <- tolower(gsub("[^a-z0-9]", "_", names(data_raw)))
  if (ncol(data_raw) > 1) {
    data_raw <- dplyr::select(data_raw, where(function(x) !all(is.na(x))))
  }
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Chromote:", nrow(data_raw), "rows from", found_url))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

