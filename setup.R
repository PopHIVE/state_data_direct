# setup.R
# Generates all 33 state respiratory data project folders under data/
# Run from state_data_direct/ directory.
# Each state gets: data/{state}_respiratory/{raw/, standard/, ingest.R, measure_info.json}

library(jsonlite)

BASE <- getwd()

# ---------------------------------------------------------------------------
# State configuration: code, FIPS, name, provider, documentation URLs, tier
# tier 1 = likely has direct data; 2 = HTML scrape attempt; 3 = dashboard-only
# strategy: "html", "arcgis", "socrata", "tier3"
# ---------------------------------------------------------------------------
states <- list(
  al = list(name="Alabama",       fips="01", provider="Alabama Department of Public Health",
    urls=c("https://www.alabamapublichealth.gov/data/respiratory.html"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv")),

  ak = list(name="Alaska",        fips="02", provider="Alaska Department of Health",
    urls=c("https://health.alaska.gov/en/resources/respiratory-virus-snapshot/"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ari","ili")),

  az = list(name="Arizona",       fips="04", provider="Arizona Department of Health Services",
    urls=c("https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/respiratory-illness/dashboards/index.php",
           "https://experience.arcgis.com/experience/8c3b1a0dc26448ccb9e1efb3b17e3a00"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ari","ili")),

  ca = list(name="California",    fips="06", provider="California Department of Public Health",
    urls=c("https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/RespiratoryVirusReport.aspx"),
    tier=1, strategy="ckan",
    ckan_base="https://data.chhs.ca.gov", ckan_dataset="respiratory-virus-dashboard",
    pathogens=c("covid","influenza","rsv")),

  co = list(name="Colorado",      fips="08", provider="Colorado Department of Public Health & Environment",
    urls=c("https://cdphe.colorado.gov/viral-respiratory-diseases-report"),
    tier=1, strategy="arcgis_direct",
    featureserver_urls=c(
      "https://services3.arcgis.com/66aUo8zsujfVXRIT/arcgis/rest/services/CDPHE_Viral_Respiratory_Homepage_/FeatureServer/0",
      "https://services3.arcgis.com/66aUo8zsujfVXRIT/arcgis/rest/services/CDPHE_Viral_Respiratory_Syndromic_Surveillance/FeatureServer/0",
      "https://services3.arcgis.com/66aUo8zsujfVXRIT/arcgis/rest/services/CDPHE_Viral_Respiratory_Sentinel_Positivity/FeatureServer/0"
    ),
    pathogens=c("covid","influenza","rsv","ili")),

  ct = list(name="Connecticut",   fips="09", provider="Connecticut Department of Public Health",
    urls=c("https://data.ct.gov/resource/8d4q-hwjx.csv"),
    tier=1, strategy="socrata",
    socrata_id="8d4q-hwjx", socrata_host="data.ct.gov",
    pathogens=c("covid","influenza","rsv")),

  fl = list(name="Florida",       fips="12", provider="Florida Department of Health",
    urls=c("https://www.floridahealth.gov/diseases-and-conditions/respiratory-illness/influenza/florida-influenza-surveillance-report-archive/index.html",
           "https://www.flhealthcharts.gov/ChartsDashboards/rdPage.aspx?rdReport=Covid19.Dataviewer"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","ili")),

  ga = list(name="Georgia",       fips="13", provider="Georgia Department of Public Health",
    urls=c("https://dph.georgia.gov/epidemiology/acute-disease-epidemiology/viral-respiratory-diseases",
           "https://wastewatersurveillance.s3.us-east-1.amazonaws.com/ExternalNWSS_20251112.html"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv")),

  ia = list(name="Iowa",          fips="19", provider="Iowa Department of Health and Human Services",
    urls=c("https://hhs.iowa.gov/health-prevention/providers-professionals/iowa-influenza-surveillance"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili","ari")),

  il = list(name="Illinois",      fips="17", provider="Illinois Department of Public Health",
    urls=c("https://dph.illinois.gov/topics-services/diseases-and-conditions/respiratory-disease/surveillance/respiratory-disease-report.html",
           "https://public.data.illinois.gov/"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ari","adenovirus","enterovirus","metapneumovirus","mycoplasma","parainfluenza","pneumonia")),

  `in` = list(name="Indiana",     fips="18", provider="Indiana Department of Health",
    urls=c("https://www.in.gov/health/directory/office-of-the-commissioner/public-health-data-navigator/infectious-disease-prevention-and-control/influenza-data-dashboard/",
           "https://www.in.gov/health/directory/office-of-the-commissioner/public-health-data-navigator/infectious-disease-prevention-and-control/covid-19-trends-and-wastewater-dashboard/"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili","ari")),

  ky = list(name="Kentucky",      fips="21", provider="Kentucky Department for Public Health",
    urls=c("https://dashboard.chfs.ky.gov/views/DPHRSP001RespiratoryDiseases/HospitalizationsandEDVisits",
           "https://dashboard.chfs.ky.gov/views/DPHRSP001RespiratoryDiseases/DeathsHistorical",
           "https://dashboard.chfs.ky.gov/views/DPHRSP001RespiratoryDiseases/Influenza-likeIllness",
           "https://dashboard.chfs.ky.gov/views/DPHRSP001RespiratoryDiseases/LaboratoryReporting"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili")),

  la = list(name="Louisiana",     fips="22", provider="Louisiana Department of Health",
    urls=c("https://ldh.la.gov/page/respiratory-hospitalization-data",
           "https://ldh.la.gov/page/respiratory-emergency-department-visits-data",
           "https://ldh.la.gov/page/respiratory-laboratory-survey-data"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv")),

  ma = list(name="Massachusetts", fips="25", provider="Massachusetts Department of Public Health",
    urls=c("https://www.mass.gov/info-details/respiratory-illness-reporting"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ari")),

  md = list(name="Maryland",      fips="24", provider="Maryland Department of Health",
    urls=c("https://health.maryland.gov/phpa/Pages/Respiratory-Virus-Surveillance.aspx",
           "https://public.tableau.com/views/MarylandCombinedRespiratoryIllness/Dashboard"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili")),

  mi = list(name="Michigan",      fips="26", provider="Michigan Department of Health and Human Services",
    urls=c("https://www.michigan.gov/mdhhs/keep-mi-healthy/infectious-diseases/seasonal-respiratory-viruses/respiratory-disease-reports"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv")),

  mn = list(name="Minnesota",     fips="27", provider="Minnesota Department of Health",
    urls=c("https://www.health.state.mn.us/diseases/respiratory/stats/lab.html",
           "https://www.health.state.mn.us/diseases/respiratory/stats/hosp.html",
           "https://www.health.state.mn.us/diseases/flu/stats/out.html",
           "https://www.health.state.mn.us/diseases/respiratory/stats/setting.html",
           "https://www.health.state.mn.us/diseases/respiratory/stats/tsys.html"),
    tier=2, strategy="html",
    pathogens=c("covid","influenza","rsv","adenovirus","coronavirus","metapneumovirus","parainfluenza","rhinovirus","ili")),

  mo = list(name="Missouri",      fips="29", provider="Missouri Department of Health & Senior Services",
    urls=c("https://health.mo.gov/living/healthcondiseases/communicable/influenza/dashboard.php"),
    tier=2, strategy="chromote",
    pathogens=c("influenza","ili")),

  nj = list(name="New Jersey",    fips="34", provider="New Jersey Department of Health",
    urls=c("https://www.nj.gov/health/respiratory-viruses/data-and-reports/#respiratory-illness-dashboard"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","adenovirus","coronavirus","enterovirus","metapneumovirus","parainfluenza","ili")),

  ny = list(name="New York",      fips="36", provider="New York State Department of Health",
    urls=c("https://health.data.ny.gov/resource/w6ed-sctw.json",
           "https://coronavirus.health.ny.gov/positive-tests-over-time-region-and-county",
           "https://coronavirus.health.ny.gov/covid-19-emergency-department-syndromic-surveillance",
           "https://coronavirus.health.ny.gov/daily-hospitalization-summary",
           "https://coronavirus.health.ny.gov/hospital-bed-capacity",
           "https://coronavirus.health.ny.gov/fatalities-0",
           "https://nyshc.health.ny.gov/web/nyapd/new-york-state-flu-tracker"),
    tier=1, strategy="socrata_multi",
    socrata_host="health.data.ny.gov",
    socrata_ids=c("w6ed-sctw","jvfi-ffup","iye6-rifr","jr8b-6gh6","cpxv-79jk"),
    socrata_descs=c("Nursing Home COVID","Statewide COVID-19 Testing","Influenza Hospitalizations",
                    "Influenza Lab Cases by County","Influenza Lab Cases by Age Group"),
    pathogens=c("covid","influenza")),

  nc = list(name="North Carolina", fips="37", provider="North Carolina Department of Health and Human Services",
    urls=c("https://covid19.ncdhhs.gov/dashboard/data-behind-dashboards"),
    tier=1, strategy="nc_special",
    pathogens=c("covid","influenza","rsv","ili")),

  oh = list(name="Ohio",          fips="39", provider="Ohio Department of Health",
    urls=c("https://data.ohio.gov/wps/portal/gov/data/view/ohio-department-of-health-respiratory-dashboard"),
    tier=1, strategy="oh_special",
    pathogens=c("covid","influenza","rsv")),

  ok = list(name="Oklahoma",      fips="40", provider="Oklahoma State Department of Health",
    urls=c("https://oklahoma.gov/health/health-education/acute-disease-service/viral-view/respiratory-data.html",
           "https://oklahoma.gov/health/health-education/acute-disease-service/viral-view/covid-19.html"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","adenovirus","coronavirus","enterovirus","metapneumovirus","mycoplasma","parainfluenza","ili")),

  or = list(name="Oregon",        fips="41", provider="Oregon Health Authority",
    urls=c("https://www.oregon.gov/oha/ph/diseasesconditions/communicabledisease/diseasesurveillancedata/influenza/pages/surveil.aspx",
           "https://www.oregon.gov/oha/ph/diseasesconditions/communicabledisease/diseasesurveillancedata/pages/respiratorysyncytialvirussurveillancedata.aspx",
           "https://public.tableau.com/views/OregonsRespiratoryVirusData/Deaths"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv")),

  pa = list(name="Pennsylvania",  fips="42", provider="Pennsylvania Department of Health",
    urls=c("https://data.pa.gov/resource/mrpb-ugjv.csv"),
    tier=1, strategy="socrata_multi",
    socrata_host="data.pa.gov",
    socrata_ids=c("mrpb-ugjv","kayn-sjhx","fbgu-sqgp","j72v-r42c","3c5w-gmss"),
    socrata_descs=c("Influenza and RSV Cases by County","COVID Hospitalizations Weekly",
                    "COVID Deaths Monthly","COVID Aggregate Cases","Wastewater Viral Activity"),
    pathogens=c("covid","influenza","rsv","ari")),

  sc = list(name="South Carolina", fips="45", provider="South Carolina Department of Public Health",
    urls=c("https://dph.sc.gov/respiratory-disease-dashboard",
           "https://dph.sc.gov/professionals/public-health-data/flu-watch"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili")),

  tn = list(name="Tennessee",     fips="47", provider="Tennessee Department of Health",
    urls=c("https://www.tn.gov/health/ceds-weeklyreports/respiratory-trends.html",
           "https://www.tn.gov/content/tn/health/cedep/immunization-program/ip/flu-in-tennessee.html"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili")),

  tx = list(name="Texas",         fips="48", provider="Texas Department of State Health Services",
    urls=c("https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/texas-statewide-hospitalization-data-for-covid19-influenza-rsv",
           "https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/texas-statewide-emergency-department-visits-for-respiratory-illnesses",
           "https://texas-respiratory-illness-dashboard-txdshsea.hub.arcgis.com/pages/viral-respiratory-deaths"),
    tier=1, strategy="arcgis_direct",
    featureserver_urls=c(
      "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_Illnesses_Graph1_Data/FeatureServer/0",
      "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_IllnessesGraph_2/FeatureServer/0",
      "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Respiratory_Illnesses_Graph_3/FeatureServer/0",
      "https://services3.arcgis.com/vljlarU2635mITsl/arcgis/rest/services/Resp_Deaths_ALL/FeatureServer/0"
    ),
    pathogens=c("covid","influenza","rsv")),

  ut = list(name="Utah",          fips="49", provider="Utah Department of Health and Human Services",
    urls=c("https://dhhs.utah.gov/health-dashboards/respiratory-disease-data/"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","ili")),

  vt = list(name="Vermont",       fips="50", provider="Vermont Department of Health",
    urls=c("https://www.healthvermont.gov/disease-control/respiratory-illnesses/respiratory-illness-activity-vermont"),
    tier=1, strategy="arcgis_direct",
    featureserver_urls=c(
      "https://maps.healthvermont.gov/arcgis/rest/services/VDH/HSIWastewaterRespIllnessSurveillance/MapServer/0"
    ),
    pathogens=c("covid","influenza","ili")),

  va = list(name="Virginia",      fips="51", provider="Virginia Department of Health",
    urls=c("https://www.vdh.virginia.gov/epidemiology/respiratory-diseases-in-virginia/data/"),
    tier=1, strategy="direct_csv",
    download_urls=c(
      "https://data.virginia.gov/dataset/31f16f23-122a-40fe-b5a3-96325c7eb6e9/resource/a0da176d-3e45-427f-a6fd-f1bcb8a71136/download/t-pud-respiratory-illness-activity.csv",
      "https://data.virginia.gov/dataset/15418583-c9b0-40a4-98dc-209f21ecb781/resource/b22fc729-830c-4a29-b088-9da5dd47be6f/download/t-pud-respiratory-emergency-department-visits.csv",
      "https://data.virginia.gov/dataset/e602390a-f4f8-46bd-a4aa-14157b7f4fde/resource/f4912847-9f9c-420e-84dd-c39df614acc0/download/t-pud-respiratory-death-data.csv",
      "https://data.virginia.gov/dataset/bbe88af0-d566-47bd-87dd-de70d164cf10/resource/bd3956ed-fbf4-4826-b6b5-c3bbe338b8d8/download/t-pud-respiratory-influenza-associated-pediatric-death.csv",
      "https://data.virginia.gov/dataset/eabea704-27d3-4d60-9078-5e1084f74db1/resource/8f013501-a5bd-45f0-9b2c-5aa09f67a04e/download/t-pud-influenza-la.csv",
      "https://data.virginia.gov/dataset/b98515e8-2004-4af1-9860-d0dab94452b4/resource/1d3d1839-5c83-4397-896a-c397926b56c4/download/vdh-t-pud-covid-lab.csv",
      "https://data.virginia.gov/dataset/cf92b99f-0dc6-496d-99a7-3be164ff78fc/resource/2855e63f-e565-4619-b003-c49141d14fd1/download/t-pud-respiratory-outbreaks.csv"
    ),
    pathogens=c("covid","influenza","rsv","ari")),

  wa = list(name="Washington",    fips="53", provider="Washington State Department of Health",
    urls=c("https://doh.wa.gov/data-and-statistical-reports/diseases-and-chronic-conditions/communicable-disease-surveillance-data/respiratory-illness-data-dashboard"),
    tier=1, strategy="direct_csv",
    download_urls=c(
      "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_CDC_Downloadable_Data.csv",
      "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_WHALES_Downloadable_Data.csv",
      "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Respiratory_Disease_RHINO_Downloadable_Data.csv",
      "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/Downloadable_Wastewater.csv",
      "https://doh.wa.gov/sites/default/files/Data/Auto-Uploads/Respiratory-Illness/wahealth_hospitaluse_download.csv"
    ),
    pathogens=c("covid","influenza","rsv")),

  wi = list(name="Wisconsin",     fips="55", provider="Wisconsin Department of Health Services",
    urls=c("https://www.dhs.wisconsin.gov/disease/laboratory-based-data.htm",
           "https://www.dhs.wisconsin.gov/disease/respiratory-emergency-department.htm",
           "https://www.dhs.wisconsin.gov/disease/respiratory-hospitalizations.htm",
           "https://www.dhs.wisconsin.gov/disease/respiratory-deaths.htm",
           "https://www.dhs.wisconsin.gov/disease/ilinet-data.htm",
           "https://www.dhs.wisconsin.gov/disease/respiratory-data.htm",
           "https://www.dhs.wisconsin.gov/wastewater/influenza-rsv-surveillance.htm"),
    tier=2, strategy="chromote",
    pathogens=c("covid","influenza","rsv","adenovirus","enterovirus","metapneumovirus","parainfluenza","pneumonia","ili","ari"))
)

# ---------------------------------------------------------------------------
# ingest.R content generators
# ---------------------------------------------------------------------------

make_header <- function(st_code, st) {
  sprintf('# ingest.R - %s (%s) Respiratory Surveillance
# Provider: %s
# Tier %d | Strategy: %s
# Run from data/%s_respiratory/

library(httr)
library(rvest)
library(vroom)
library(dplyr)
library(jsonlite)

state_fips  <- "%s"
state_name  <- "%s"
source_urls <- c(%s)

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

',
    st$name, toupper(st_code), st$provider, st$tier, st$strategy,
    st_code, st$fips, st$name,
    paste0('"', st$urls, '"', collapse=", ")
  )
}

make_footer <- function() {
'
# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox=TRUE, pretty=TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\\n",
    state_name, result$success, result$rows, result$message))
'
}

make_html_body <- function() {
'result <- tryCatch({
  found_file <- NULL

  for (url in source_urls) {
    resp <- tryCatch(httr::GET(url, httr::timeout(30),
      httr::user_agent("Mozilla/5.0 (compatible; R scraper)")),
      error = function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next

    html  <- rvest::read_html(httr::content(resp, "text", encoding="UTF-8"))
    links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
    links <- links[!is.na(links)]

    # Find direct data file links
    data_links <- links[grepl("[.](csv|xlsx?|json|tsv|xls)([?]|$|#)", links, ignore.case=TRUE)]

    if (length(data_links) > 0) {
      # Resolve relative URLs
      base <- httr::parse_url(url)
      data_links <- sapply(data_links, function(l) {
        if (grepl("^https?://", l)) l
        else if (grepl("^//", l)) paste0(base$scheme, ":", l)
        else if (grepl("^/", l)) paste0(base$scheme, "://", base$hostname, l)
        else paste0(url, "/", l)
      })
      found_file <- data_links[1]
      break
    }
  }

  if (is.null(found_file)) stop("No CSV/Excel/JSON download links found on any source page")

  # Extract file extension safely from URL path (not query string)
  url_path <- tryCatch(httr::parse_url(found_file)$path, error=function(e) found_file)
  ext <- tolower(sub(".*[.]([a-zA-Z0-9]{1,5})$", "\\1", url_path))
  if (!ext %in% c("csv","xlsx","xls","json","tsv","zip")) ext <- "csv"
  local_file <- paste0("raw/download.", ext)
  dl_resp <- httr::GET(found_file, httr::timeout(60),
    httr::write_disk(local_file, overwrite=TRUE),
    httr::user_agent("Mozilla/5.0 (compatible; R scraper)"))
  if (httr::status_code(dl_resp) != 200) stop(paste("Download failed:", httr::status_code(dl_resp)))

  # Load the file
  data_raw <- if (ext %in% c("csv","tsv")) {
    vroom::vroom(local_file, show_col_types=FALSE)
  } else if (ext == "json") {
    as.data.frame(jsonlite::fromJSON(local_file))
  } else {
    stop(paste("Unsupported file type:", ext))
  }

  # Minimal standardization: ensure geography and time columns
  col_names_lower <- tolower(names(data_raw))
  names(data_raw)  <- col_names_lower

  # Reject empty datasets
  if (nrow(data_raw) == 0) stop("Downloaded file has 0 rows of data")

  # Validate this is respiratory surveillance data (not vaccine schedules, etc.)
  resp_keywords <- c("influenza", "rsv", "covid", "respiratory", "positive",
                     "cases", "surveillance", "ili", "ari", "pcr", "antigen",
                     "virus", "percent", "hosp", "death", "flu")
  col_text <- paste(tolower(names(data_raw)), collapse=" ")
  col_match <- any(sapply(resp_keywords, function(k) grepl(k, col_text)))
  if (!col_match) {
    sample_text <- paste(tolower(unlist(head(data_raw, 3))), collapse=" ")
    content_match <- any(sapply(resp_keywords, function(k) grepl(k, sample_text)))
    if (!content_match) stop("Downloaded data does not appear to be respiratory surveillance data")
  }

  # Write raw-as-standard for now (geography/time mapping depends on source format)
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")

  process$raw_state <- tools::md5sum(local_file)[[1]]
  process$success   <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Downloaded", nrow(data_raw), "rows from", found_file))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
'
}

make_arcgis_body <- function(st_code, st) {
  paste0('
# ArcGIS Hub page - try to find underlying FeatureServer REST API
result <- tryCatch({
  found_data <- FALSE
  data_raw   <- NULL

  for (hub_url in source_urls) {
    # Try fetching the Hub page to find ArcGIS service URLs
    resp <- tryCatch(httr::GET(hub_url, httr::timeout(30),
      httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
    if (is.null(resp) || httr::status_code(resp) != 200) next

    page_text <- httr::content(resp, "text", encoding="UTF-8")

    # Extract FeatureServer URLs from page source
    fs_matches <- regmatches(page_text,
      gregexpr("https?://[^[:space:]]+/FeatureServer/[0-9]+", page_text))[[1]]
    fs_matches <- unique(fs_matches)

    # Also try direct CSV download links
    html  <- rvest::read_html(page_text)
    links <- html |> rvest::html_nodes("a") |> rvest::html_attr("href")
    csv_links <- links[grepl("[.]csv", links, ignore.case=TRUE) & !is.na(links)]

    if (length(csv_links) > 0) {
      dl_url <- csv_links[1]
      if (!grepl("^http", dl_url)) dl_url <- paste0("https://", httr::parse_url(hub_url)$hostname, dl_url)
      dl_resp <- httr::GET(dl_url, httr::timeout(60), httr::write_disk("raw/data.csv", overwrite=TRUE))
      if (httr::status_code(dl_resp) == 200) {
        data_raw <- vroom::vroom("raw/data.csv", show_col_types=FALSE)
        found_data <- TRUE
        break
      }
    }

    for (fs_url in fs_matches[seq_len(min(3, length(fs_matches)))]) {
      query_url <- paste0(fs_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
      fs_resp <- tryCatch(httr::GET(query_url, httr::timeout(30)), error=function(e) NULL)
      if (is.null(fs_resp) || httr::status_code(fs_resp) != 200) next
      fs_data <- tryCatch(jsonlite::fromJSON(httr::content(fs_resp, "text")), error=function(e) NULL)
      if (!is.null(fs_data$features) && nrow(fs_data$features) > 0) {
        data_raw <- as.data.frame(fs_data$features$attributes)
        found_data <- TRUE
        break
      }
    }
    if (found_data) break
  }

  if (!found_data || is.null(data_raw)) stop("ArcGIS FeatureServer not found or returned no data")

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("ArcGIS data retrieved:", nrow(data_raw), "rows"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
')
}

make_tier3_body <- function(reason) {
  sprintf('
# This source requires a JavaScript-rendered dashboard and cannot be scraped
# programmatically with standard R HTTP tools.
result <- list(
  success = FALSE,
  rows    = 0L,
  message = "%s"
)
process$success <- FALSE
',
  reason)
}

make_nc_body <- function() {
'
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
'
}

make_oh_body <- function() {
'
# Ohio - data.ohio.gov open data portal (Socrata-based)
result <- tryCatch({
  # Try to find Socrata dataset ID from the portal page
  portal_url <- source_urls[1]
  resp <- httr::GET(portal_url, httr::timeout(30), httr::user_agent("Mozilla/5.0"))

  # data.ohio.gov uses a custom portal; try known dataset patterns
  # Search for JSON API endpoint via page scraping
  page_text <- httr::content(resp, "text", encoding="UTF-8")

  # Extract dataset IDs (Socrata format: 4-char-4-char)
  id_matches <- regmatches(page_text, gregexpr("[a-z0-9]{4}-[a-z0-9]{4}", page_text))[[1]]
  id_matches <- unique(id_matches)

  # Also try the OAKS (Ohio portal) API endpoint pattern
  # Try a direct search via data.ohio.gov catalog API
  search_resp <- tryCatch(
    httr::GET("https://data.ohio.gov/api/catalog/v1?search=respiratory+health&limit=10",
              httr::timeout(30)),
    error=function(e) NULL)

  found_data <- FALSE
  data_raw   <- NULL

  if (!is.null(search_resp) && httr::status_code(search_resp) == 200) {
    catalog <- tryCatch(jsonlite::fromJSON(httr::content(search_resp, "text")), error=function(e) NULL)
    if (!is.null(catalog$results) && length(catalog$results) > 0) {
      # Try first matching dataset
      ds_id <- catalog$results$id[1]
      api_url <- paste0("https://data.ohio.gov/resource/", ds_id, ".json?$limit=50000")
      data_resp <- httr::GET(api_url, httr::timeout(60))
      if (httr::status_code(data_resp) == 200) {
        data_raw <- as.data.frame(jsonlite::fromJSON(httr::content(data_resp, "text")))
        found_data <- TRUE
      }
    }
  }

  # Try CSV download directly
  if (!found_data) {
    # Try common Ohio respiratory dataset ID patterns
    candidate_ids <- c("4zti-3ab3", "qtaz-5kaw", "rdvy-kkni")
    for (ds_id in candidate_ids) {
      api_url  <- paste0("https://data.ohio.gov/resource/", ds_id, ".csv?$limit=50000")
      dl_resp  <- tryCatch(httr::GET(api_url, httr::timeout(30),
                    httr::write_disk("raw/data.csv", overwrite=TRUE)), error=function(e) NULL)
      if (!is.null(dl_resp) && httr::status_code(dl_resp) == 200) {
        d <- tryCatch(vroom::vroom("raw/data.csv", show_col_types=FALSE), error=function(e) NULL)
        if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE; break }
      }
    }
  }

  if (!found_data) stop("Ohio data.ohio.gov dataset not accessible via API; dataset ID unknown")

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Ohio data retrieved:", nrow(data_raw), "rows"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
'
}

make_socrata_body <- function(st) {
  sprintf('
# Socrata open data portal: %s
result <- tryCatch({
  socrata_id   <- "%s"
  socrata_host <- "%s"

  # Try CSV endpoint first (more reliable for bulk data)
  csv_url <- sprintf("https://%%s/resource/%%s.csv?$limit=50000", socrata_host, socrata_id)
  resp <- httr::GET(csv_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"))

  if (httr::status_code(resp) == 200) {
    writeBin(httr::content(resp, "raw"), "raw/data.csv")
    data_raw <- vroom::vroom("raw/data.csv", show_col_types=FALSE)
  } else {
    # Fallback to JSON endpoint
    json_url <- sprintf("https://%%s/resource/%%s.json?$limit=50000", socrata_host, socrata_id)
    resp <- httr::GET(json_url, httr::timeout(60), httr::user_agent("Mozilla/5.0"))
    if (httr::status_code(resp) != 200) stop(paste("Socrata API returned HTTP", httr::status_code(resp)))
    data_raw <- as.data.frame(jsonlite::fromJSON(httr::content(resp, "text")))
  }

  if (nrow(data_raw) == 0) stop("Socrata API returned 0 rows")

  names(data_raw) <- tolower(gsub("[^a-z0-9]", "_", names(data_raw)))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Socrata data:", nrow(data_raw), "rows from", socrata_id))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
',
  st$socrata_host, st$socrata_id, st$socrata_host)
}

make_direct_csv_body <- function(st) {
  dl_urls_str <- paste0('"', st$download_urls, '"', collapse=", ")
  sprintf('
# Direct CSV download: %s
result <- tryCatch({
  dl_urls <- c(%s)
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
       message=paste("Direct CSV:", nrow(data_raw), "rows from", length(all_data), "files"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
',
  st$name, dl_urls_str)
}

make_ckan_body <- function(st) {
  sprintf('
# CKAN open data portal: %s
result <- tryCatch({
  ckan_base    <- "%s"
  ckan_dataset <- "%s"

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
',
  st$ckan_base, st$ckan_base, st$ckan_dataset)
}

make_arcgis_direct_body <- function(st) {
  fs_urls_str <- paste0('"', st$featureserver_urls, '"', collapse=", ")
  sprintf('
# ArcGIS FeatureServer direct query: %s
result <- tryCatch({
  fs_urls <- c(%s)
  found_data <- FALSE
  data_raw   <- NULL
  all_data   <- list()

  for (fs_url in fs_urls) {
    # Query FeatureServer layer
    query_url <- paste0(fs_url, "/query?where=1%%3D1&outFields=*&f=json&resultRecordCount=50000")
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
',
  st$name, fs_urls_str)
}

make_tableau_export_body <- function(st) {
  host <- if (!is.null(st$tableau_host)) st$tableau_host else "public.tableau.com"
  workbook <- st$tableau_workbook
  sprintf('
# Tableau dashboard export: %s
result <- tryCatch({
  tableau_host <- "%s"
  workbook     <- "%s"

  found_data <- FALSE
  data_raw   <- NULL

  # Strategy 1: Tableau Public crosstab CSV download
  csv_url <- paste0("https://", tableau_host, "/views/", workbook, ".csv")
  csv_resp <- tryCatch(httr::GET(csv_url, httr::timeout(30),
    httr::write_disk("raw/tableau_data.csv", overwrite=TRUE),
    httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
  if (!is.null(csv_resp) && httr::status_code(csv_resp) == 200) {
    d <- tryCatch(vroom::vroom("raw/tableau_data.csv", show_col_types=FALSE), error=function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE }
  }

  # Strategy 2: Try underlying data endpoint
  if (!found_data) {
    data_url <- paste0("https://", tableau_host, "/vizql/w/",
      sub("/.*", "", workbook), "/v/", sub(".*/", "", workbook),
      "/viewData/download?format=csv")
    data_resp <- tryCatch(httr::GET(data_url, httr::timeout(30),
      httr::write_disk("raw/tableau_data.csv", overwrite=TRUE),
      httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
    if (!is.null(data_resp) && httr::status_code(data_resp) == 200) {
      d <- tryCatch(vroom::vroom("raw/tableau_data.csv", show_col_types=FALSE), error=function(e) NULL)
      if (!is.null(d) && nrow(d) > 0) { data_raw <- d; found_data <- TRUE }
    }
  }

  if (!found_data) stop("Tableau dashboard does not support programmatic CSV export")

  names(data_raw) <- tolower(names(data_raw))
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim=",")
  process$success <- TRUE

  list(success=TRUE, rows=nrow(data_raw),
       message=paste("Tableau export:", nrow(data_raw), "rows"))

}, error=function(e) {
  list(success=FALSE, rows=0L, message=conditionMessage(e))
})
',
  host, host, workbook)
}

make_chromote_body <- function(st_name="") {
  paste0('
# Chromote headless browser: ', st_name, '
# Strategy: render page with Chrome, extract network API calls via Performance API,
# replay them with httr to find respiratory data. Falls back to HTML parsing.
result <- tryCatch({
  library(chromote)

  b <- ChromoteSession$new()
  on.exit(tryCatch(b$close(), error=function(e) NULL), add=TRUE)

  data_raw   <- NULL
  found_data <- FALSE
  found_url  <- NULL
  resp_kw <- c("influenza","rsv","covid","respiratory","ili","ari",
               "surveillance","virus","cases","percent","hosp","death","flu","disease")

  # Parse JSON text into a data frame (handles ArcGIS, Socrata, plain arrays)
  parse_json_df <- function(txt) {
    tryCatch({
      p <- jsonlite::fromJSON(txt, flatten=TRUE)
      if (is.data.frame(p) && nrow(p) > 0) return(p)
      if (!is.null(p$features) && is.data.frame(p$features$attributes)) return(p$features$attributes)
      if (!is.null(p$features) && length(p$features) > 0) return(tryCatch(as.data.frame(p$features$attributes), error=function(e) NULL))
      for (k in c("data","value","result","results","rows","records","items")) {
        if (!is.null(p[[k]]) && is.data.frame(p[[k]]) && nrow(p[[k]]) > 0) return(p[[k]])
      }
      if (is.list(p) && length(p) > 0 && is.list(p[[1]])) {
        d <- tryCatch(dplyr::bind_rows(lapply(p, as.data.frame)), error=function(e) NULL)
        if (!is.null(d) && nrow(d) > 0) return(d)
      }
      NULL
    }, error=function(e) NULL)
  }

  is_resp <- function(d) {
    if (is.null(d) || nrow(d) == 0) return(FALSE)
    txt <- paste(c(tolower(names(d)), tolower(unlist(head(d, 2)))), collapse=" ")
    any(sapply(resp_kw, function(k) grepl(k, txt, fixed=TRUE)))
  }

  for (page_url in source_urls) {
    # Navigate and wait for page + async data calls to complete
    tryCatch(b$Page$navigate(page_url, wait_=TRUE, timeout_=25), error=function(e) NULL)
    Sys.sleep(12)

    # ---- Strategy A: Performance API — extract all XHR/Fetch URLs the page called ----
    perf_js <- "
      (function() {
        try {
          var entries = window.performance.getEntriesByType(\"resource\");
          var urls = entries.filter(function(e) {
            var u = e.name;
            var isData = (e.initiatorType === \"xmlhttprequest\" ||
                          e.initiatorType === \"fetch\" ||
                          u.indexOf(\"/query?\") >= 0 ||
                          u.indexOf(\"/resource/\") >= 0 ||
                          /[.](csv|json)([?#]|$)/i.test(u));
            var isAsset = /[.](js|css|png|jpg|gif|woff2?|ico|svg|wasm|map)([?#]|$)/i.test(u) ||
                          /analytics|tracking|beacon|sentry|telemetry|newrelic/i.test(u);
            return isData && !isAsset;
          }).map(function(e) { return e.name; });
          return JSON.stringify(urls);
        } catch(ex) { return \"[]\"; }
      })()
    "
    perf_result <- tryCatch(
      b$Runtime$evaluate(perf_js)$result$value,
      error=function(e) "[]")
    api_urls <- tryCatch(unique(jsonlite::fromJSON(perf_result)), error=function(e) character(0))

    for (api_url in api_urls[seq_len(min(40, length(api_urls)))]) {
      r <- tryCatch(httr::GET(api_url, httr::timeout(30),
        httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
      if (is.null(r) || httr::status_code(r) != 200) next
      ctype <- httr::headers(r)[["content-type"]]
      ctype <- if (is.null(ctype)) "" else ctype
      txt   <- tryCatch(httr::content(r, "text"), error=function(e) "")
      d <- NULL
      if (grepl("json", ctype, ignore.case=TRUE)) {
        d <- parse_json_df(txt)
      } else {
        d <- tryCatch(vroom::vroom(I(txt), show_col_types=FALSE), error=function(e) NULL)
        if (is.null(d)) d <- parse_json_df(txt)
      }
      if (is_resp(d)) { data_raw <- d; found_data <- TRUE; found_url <- api_url; break }
    }
    if (found_data) break

    # ---- Strategy B: Parse rendered HTML ----
    html_content <- tryCatch(
      b$Runtime$evaluate("document.documentElement.outerHTML")$result$value,
      error=function(e) "")
    if (nchar(html_content) < 200) next
    html_dom <- tryCatch(rvest::read_html(html_content), error=function(e) NULL)

    # B1: Download links in rendered HTML
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
        ext <- tolower(sub(".*[.]([a-zA-Z0-9]{1,5})([?#].*)?$", "\\\\1", dl_url))
        if (!ext %in% c("csv","xlsx","xls","json","tsv")) ext <- "csv"
        local_f <- paste0("raw/chromote_dl.", ext)
        dl_resp <- tryCatch(httr::GET(dl_url, httr::timeout(90),
          httr::write_disk(local_f, overwrite=TRUE),
          httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(dl_resp) || httr::status_code(dl_resp) != 200) next
        d <- if (ext %in% c("xlsx","xls")) {
          tryCatch(as.data.frame(readxl::read_excel(local_f)), error=function(e) NULL)
        } else if (ext == "json") {
          parse_json_df(paste(readLines(local_f, warn=FALSE), collapse="\n"))
        } else {
          tryCatch(vroom::vroom(local_f, show_col_types=FALSE), error=function(e) NULL)
        }
        if (is_resp(d)) { data_raw <- d; found_data <- TRUE; found_url <- dl_url; break }
      }
      if (found_data) break
    }

    # B2: ArcGIS FeatureServer/MapServer in page source
    for (pat in c("/FeatureServer/[0-9]+", "/MapServer/[0-9]+")) {
      if (found_data) break
      svc_urls <- unique(regmatches(html_content,
        gregexpr(paste0("https?://[^[:space:]]+", pat), html_content))[[1]])
      for (svc_url in svc_urls[seq_len(min(5, length(svc_urls)))]) {
        q_url <- paste0(svc_url, "/query?where=1%3D1&outFields=*&f=json&resultRecordCount=50000")
        r <- tryCatch(httr::GET(q_url, httr::timeout(60), httr::user_agent("Mozilla/5.0")), error=function(e) NULL)
        if (is.null(r) || httr::status_code(r) != 200) next
        d <- parse_json_df(httr::content(r, "text"))
        if (is_resp(d)) { data_raw <- d; found_data <- TRUE; found_url <- svc_url; break }
      }
    }
    if (found_data) break

    # B3: Socrata endpoints in page source
    soc_urls <- unique(regmatches(html_content,
      gregexpr("https?://[a-zA-Z0-9.-]+[.]gov/resource/[a-z0-9]{4}-[a-z0-9]{4}", html_content))[[1]])
    for (soc_base in soc_urls[seq_len(min(3, length(soc_urls)))]) {
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

  if (!found_data) stop("Chromote: no data found via network replay or HTML parsing")

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
')
}

# ---------------------------------------------------------------------------
# measure_info.json generator
# ---------------------------------------------------------------------------
make_measure_info <- function(st_code, st) {
  source_key <- paste0(st_code, "_dph")
  pathogen_measures <- lapply(st$pathogens, function(p) {
    list(
      id              = paste0(p, "_surveillance"),
      short_name      = paste(toupper(p), "surveillance"),
      long_name       = paste(st$name, toupper(p), "Respiratory Surveillance"),
      category        = "respiratory",
      short_description = paste("Weekly", p, "surveillance data from", st$provider),
      long_description  = paste(
        st$provider, "collects and reports", p,
        "surveillance data weekly. Data covers emergency department visits,",
        "hospitalizations, laboratory-confirmed cases, and/or percent positivity."),
      statement       = paste("{geography} reported {value} for", p, "surveillance in {time}."),
      measure_type    = "Count or Percent (source-dependent)",
      unit            = "Varies by indicator",
      time_resolution = "Week",
      sources         = list(list(id=source_key)),
      citations       = list()
    )
  })
  names(pathogen_measures) <- paste0(st$pathogens, "_surveillance")

  sources_obj <- list()
  sources_obj[[source_key]] <- list(
    name             = st$provider,
    url              = st$urls[1],
    organization     = st$provider,
    organization_url = paste0("https://", httr::parse_url(st$urls[1])$hostname),
    description      = paste(
      st$provider, "provides weekly respiratory surveillance data for", st$name,
      "covering pathogens including:", paste(st$pathogens, collapse=", "),
      ". Data is published via state health department dashboards and reports.",
      "Temporal coverage: ~2019-ongoing. Geographic coverage: state level."),
    restrictions = "Public domain state government health data.",
    date_accessed = 2026
  )

  result <- c(pathogen_measures, list(`_sources`=sources_obj))
  result
}

# ---------------------------------------------------------------------------
# Main loop: create all state project folders
# ---------------------------------------------------------------------------
cat("Creating state project folders...\n")
created <- 0

for (st_code in names(states)) {
  st      <- states[[st_code]]
  dir_name <- paste0(st_code, "_respiratory")
  dir_path <- file.path("data", dir_name)

  dir.create(file.path(dir_path, "raw"),      recursive=TRUE, showWarnings=FALSE)
  dir.create(file.path(dir_path, "standard"), recursive=TRUE, showWarnings=FALSE)

  # Build ingest.R content
  header <- make_header(st_code, st)
  body   <- switch(st$strategy,
    "html"           = make_html_body(),
    "arcgis"         = make_arcgis_body(st_code, st),
    "tier3"          = make_tier3_body(st$tier3_reason),
    "nc_special"     = make_nc_body(),
    "oh_special"     = make_oh_body(),
    "socrata"        = make_socrata_body(st),
    "ckan"           = make_ckan_body(st),
    "arcgis_direct"  = make_arcgis_direct_body(st),
    "direct_csv"     = make_direct_csv_body(st),
    "tableau_export" = make_tableau_export_body(st),
    "chromote"       = make_chromote_body(st$name),
    make_html_body()
  )
  footer <- make_footer()

  writeLines(paste0(header, body, footer), file.path(dir_path, "ingest.R"))

  # Write measure_info.json
  measure_info <- make_measure_info(st_code, st)
  write(toJSON(measure_info, auto_unbox=TRUE, pretty=TRUE),
        file.path(dir_path, "measure_info.json"))

  cat(sprintf("  [%s] Created %s (Tier %d, %s)\n", st_code, dir_name, st$tier, st$strategy))
  created <- created + 1
}

cat(sprintf("\nDone. Created %d state projects in data/\n", created))
