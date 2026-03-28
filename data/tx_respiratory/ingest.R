# ingest.R - Texas (TX) Respiratory Surveillance
# Provider: Texas Department of State Health Services
# Tier 1 | Strategy: arcgis_direct
# Run from data/tx_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "48"
state_name  <- "Texas"
source_urls <- c("https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/texas-statewide-hospitalization-data-for-covid19-influenza-rsv", "https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/texas-statewide-emergency-department-visits-for-respiratory-illnesses", "https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/viral-respiratory-deaths")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}


# ArcGIS FeatureServer direct query: Texas
result <- tryCatch({
  # Graph1=hospitalization by age, Graph2=ED visits by age, Graph3=% positivity, Resp_Deaths_ALL=% deaths
  fs_urls <- c(
    "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_Illnesses_Graph1_Data/FeatureServer/0",
    "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_IllnessesGraph_2/FeatureServer/0",
    "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_Illnesses_Graph_3/FeatureServer/0",
    "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Resp_Deaths_ALL/FeatureServer/0"
  )
  found_data <- FALSE
  data_raw   <- NULL
  all_data   <- list()

  for (fs_url in fs_urls) {
    # Query FeatureServer layer
    query_url <- paste0(fs_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
    resp <- tryCatch(httr::GET(query_url, httr::timeout(60),
      httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next

    json_data <- tryCatch(jsonlite::fromJSON(httr::content(resp, "text")), error=function(e) NULL)
    if (!is.null(json_data$features) && length(json_data$features) > 0) {
      d <- as.data.frame(json_data$features$attributes)
      if (nrow(d) > 0) all_data[[length(all_data)+1]] <- d
    }
  }

  if (length(all_data) == 0) stop("ArcGIS FeatureServer returned no data")

  # Combine all layers (coerce all columns to character to avoid type conflicts)
  data_raw <- all_data[[1]]
  if (length(all_data) > 1) {
    all_data <- lapply(all_data, function(d) dplyr::mutate(d, dplyr::across(dplyr::everything(), as.character)))
    data_raw <- dplyr::bind_rows(all_data)
  }

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("ArcGIS FeatureServer data:", nrow(data_raw), "rows from", length(all_data), "layers"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))

