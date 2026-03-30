# ingest.R - Arizona (AZ) Respiratory Surveillance
# Provider: Arizona Department of Health Services
# Tier 2 | Strategy: chromote (Power BI public API)
# Run from data/az_respiratory/

library(httr2)
library(jsonlite)
library(dplyr)
library(vroom)
library(lubridate)

state_fips  <- "04"
state_name  <- "Arizona"
source_urls <- c("https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/respiratory-illness/dashboards/index.php")

# Initialize process record
process_file <- "process.json"
if (file.exists(process_file)) {
  process <- fromJSON(process_file)
} else {
  process <- list(raw_state = NULL, last_run = NULL, success = FALSE)
}

result <- tryCatch({

  # ================================================================
  # Power BI API configuration
  # ================================================================
  api_url <- paste0(
    "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net",
    "/public/reports/querydata?synchronous=true"
  )
  resource_key <- "733d649c-e71b-4cbc-a2ae-fb86d7e480d2"
  dataset_id <- "f3cd4838-0724-4753-b65f-0bec3f4a71f2"
  report_id <- "a573a223-c57f-498f-86a4-b35c27a62558"
  visual_id <- "1cd7ecf0e0d00b91ae70"
  model_id <- 791111

  headers <- c(
    "Content-Type" = "application/json;charset=UTF-8",
    "X-PowerBI-ResourceKey" = resource_key,
    "User-Agent" = paste0(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
      "AppleWebKit/537.36"
    )
  )

  # --- Helpers to build PBI query payloads ---

  col_ref <- function(alias, property) {
    list(Column = list(
      Expression = list(
        SourceRef = list(Source = alias)
      ),
      Property = property
    ))
  }

  msr_ref <- function(alias, property) {
    list(Measure = list(
      Expression = list(
        SourceRef = list(Source = alias)
      ),
      Property = property
    ))
  }

  build_payload <- function(from_list, select_list,
                            top_count = 30000L) {
    n <- length(select_list)
    list(
      version = "1.0.0",
      queries = I(list(list(
        Query = list(Commands = I(list(list(
          SemanticQueryDataShapeCommand = list(
            Query = list(
              Version = 2,
              From = from_list,
              Select = select_list
            ),
            Binding = list(
              Primary = list(
                Groupings = I(list(list(
                  Projections = seq(0L, n - 1L)
                )))
              ),
              DataReduction = list(
                DataVolume = 4,
                Primary = list(
                  Top = list(Count = top_count)
                )
              ),
              Version = 1
            )
          )
        )))),
        ApplicationContext = list(
          DatasetId = dataset_id,
          Sources = I(list(list(
            ReportId = report_id,
            VisualId = visual_id
          )))
        )
      ))),
      modelId = model_id
    )
  }

  execute_query <- function(payload) {
    pjson <- toJSON(payload, auto_unbox = TRUE)
    resp <- request(api_url) |>
      req_headers(!!!headers) |>
      req_body_raw(pjson, type = "application/json") |>
      req_error(is_error = \(r) FALSE) |>
      req_perform()
    if (resp_status(resp) != 200) {
      warning("HTTP ", resp_status(resp))
      return(NULL)
    }
    resp_body_json(resp, check_type = FALSE)
  }

  # --- DSR delta decoder ---

  decode_dsr <- function(rj, col_names) {
    ds <- rj$results[[1]]$result$data$dsr$DS[[1]]
    rows <- ds$PH[[1]]$DM0
    if (is.null(rows) || length(rows) == 0) return(NULL)

    dicts <- ds$ValueDicts
    schema <- rows[[1]]$S
    n_cols <- length(schema)

    col_dicts <- vapply(
      schema,
      \(s) if (!is.null(s$DN)) s$DN else NA_character_,
      character(1)
    )

    bits <- 2L^(seq_len(n_cols) - 1L)
    prev_vals <- rep(list(NA), n_cols)

    decoded <- vector("list", length(rows))
    for (ri in seq_along(rows)) {
      row <- rows[[ri]]
      r_mask <- row$R %||% 0L
      phi_mask <- row[["\u00d8"]] %||% 0L

      cur <- prev_vals
      for (ci in which(bitwAnd(phi_mask, bits) != 0L))
        cur[[ci]] <- NA
      new_idx <- which(
        bitwAnd(r_mask, bits) == 0L &
          bitwAnd(phi_mask, bits) == 0L
      )
      c_vals <- row$C %||% list()
      for (i in seq_along(new_idx)) {
        if (i <= length(c_vals))
          cur[[new_idx[i]]] <- c_vals[[i]]
      }
      prev_vals <- cur

      for (ci in seq_len(n_cols)) {
        dn <- col_dicts[ci]
        if (!is.na(dn) && !is.na(cur[[ci]])) {
          idx <- as.integer(cur[[ci]]) + 1L
          d <- dicts[[dn]]
          if (!is.null(d) && idx >= 1 && idx <= length(d))
            cur[[ci]] <- d[[idx]]
        }
      }
      decoded[[ri]] <- cur
    }

    df <- do.call(rbind, lapply(decoded, function(vals) {
      as.data.frame(
        setNames(
          lapply(vals, \(v) if (is.null(v)) NA else v),
          col_names[seq_len(n_cols)]
        ),
        stringsAsFactors = FALSE
      )
    }))
    df
  }

  # --- Entity/alias shorthands ---

  from_rsv <- list(
    Name = "p",
    Entity = "Pub1ic respiratory_rsv_case_data",
    Type = 0
  )
  from_covid <- list(
    Name = "p1",
    Entity = "Pub1ic respiratory_covid_case_data",
    Type = 0
  )
  from_flu <- list(
    Name = "p2",
    Entity = "Pub1ic respiratory_flu_case_data",
    Type = 0
  )
  from_dates <- list(
    Name = "t", Entity = "Table_Startdate", Type = 0
  )
  from_county_season <- list(
    Name = "tc", Entity = "Table_County_Season", Type = 0
  )
  from_essence <- list(
    Name = "r", Entity = "respiratory_Essence", Type = 0
  )
  from_death <- list(
    Name = "d",
    Entity = "Pub1ic respiratory_death_data",
    Type = 0
  )

  convert_pbi_date <- function(x) {
    as_date(as_datetime(as.numeric(x) / 1000))
  }

  all_data <- list()

  # ================================================================
  # Q1: Statewide case counts by week + season
  # ================================================================
  cat("Q1: Statewide cases by week + season...\n")

  q1 <- build_payload(
    from_list = list(
      from_rsv, from_covid, from_flu,
      from_dates, from_county_season
    ),
    select_list = list(
      col_ref("tc", "SEASON"),
      col_ref("t", "startdate"),
      msr_ref("p1", "COVID-19 DC"),
      msr_ref("p", "RSV DC MEDSISID"),
      msr_ref("p2", "Flu DC MEDSISID")
    )
  )
  q1_rj <- execute_query(q1)
  cases_state <- decode_dsr(q1_rj, c(
    "season", "startdate", "covid_cases",
    "rsv_cases", "flu_cases"
  ))
  if (!is.null(cases_state)) {
    cases_state$startdate <- convert_pbi_date(cases_state$startdate)
    cat("  ", nrow(cases_state), "rows\n")
    all_data[["cases_statewide"]] <- cases_state
  }

  # ================================================================
  # Q2: Case counts by week + county (one per disease)
  # ================================================================

  query_by_county <- function(entity_from, alias,
                              measure, disease) {
    cat("Q2:", disease, "by county...\n")
    p <- build_payload(
      from_list = list(
        entity_from, from_dates, from_county_season
      ),
      select_list = list(
        col_ref("tc", "COUNTYNM"),
        col_ref("tc", "SEASON"),
        col_ref("t", "startdate"),
        msr_ref(alias, measure)
      )
    )
    rj <- execute_query(p)
    df <- decode_dsr(rj, c(
      "county", "season", "startdate", "case_count"
    ))
    if (!is.null(df)) {
      df$startdate <- convert_pbi_date(df$startdate)
      df$disease <- disease
      cat("  ", nrow(df), "rows\n")
    }
    df
  }

  flu_county <- query_by_county(
    from_flu, "p2", "Flu DC MEDSISID", "Influenza"
  )
  covid_county <- query_by_county(
    from_covid, "p1", "COVID-19 DC", "COVID-19"
  )
  rsv_county <- query_by_county(
    from_rsv, "p", "RSV DC MEDSISID", "RSV"
  )
  cases_county <- bind_rows(flu_county, covid_county, rsv_county)
  if (!is.null(cases_county) && nrow(cases_county) > 0) {
    all_data[["cases_by_county"]] <- cases_county
  }

  # ================================================================
  # Q3: Case counts by week + age group
  # ================================================================

  query_by_age <- function(entity_name, alias,
                           measure, disease) {
    cat("Q3:", disease, "by age group...\n")
    from_e <- list(
      Name = alias, Entity = entity_name, Type = 0
    )
    p <- build_payload(
      from_list = list(from_e, from_dates),
      select_list = list(
        col_ref(alias, "AGEGP"),
        col_ref(alias, "SEASON"),
        col_ref("t", "startdate"),
        msr_ref(alias, measure)
      )
    )
    rj <- execute_query(p)
    df <- decode_dsr(rj, c(
      "age_group", "season", "startdate", "case_count"
    ))
    if (!is.null(df)) {
      df$startdate <- convert_pbi_date(df$startdate)
      df$disease <- disease
      cat("  ", nrow(df), "rows\n")
    }
    df
  }

  flu_age <- query_by_age(
    "Pub1ic respiratory_flu_case_data", "f",
    "Flu DC MEDSISID", "Influenza"
  )
  covid_age <- query_by_age(
    "Pub1ic respiratory_covid_case_data", "c",
    "COVID-19 DC", "COVID-19"
  )
  rsv_age <- query_by_age(
    "Pub1ic respiratory_rsv_case_data", "r",
    "RSV DC MEDSISID", "RSV"
  )
  cases_age <- bind_rows(flu_age, covid_age, rsv_age)
  if (!is.null(cases_age) && nrow(cases_age) > 0) {
    all_data[["cases_by_age"]] <- cases_age
  }

  # ================================================================
  # Q4: Flu type/subtype breakdown by week + season
  # ================================================================
  cat("Q4: Flu type/subtype breakdown...\n")

  q4 <- build_payload(
    from_list = list(
      list(
        Name = "f",
        Entity = "Pub1ic respiratory_flu_case_data",
        Type = 0
      ),
      from_dates
    ),
    select_list = list(
      col_ref("f", "TYPE"),
      col_ref("f", "SUBTYPE"),
      col_ref("f", "SEASON"),
      col_ref("t", "startdate"),
      msr_ref("f", "Flu DC MEDSISID")
    )
  )
  q4_rj <- execute_query(q4)
  flu_types <- decode_dsr(q4_rj, c(
    "flu_type", "flu_subtype", "season",
    "startdate", "case_count"
  ))
  if (!is.null(flu_types)) {
    flu_types$startdate <- convert_pbi_date(flu_types$startdate)
    cat("  ", nrow(flu_types), "rows\n")
    all_data[["flu_type_subtype"]] <- flu_types
  }

  # ================================================================
  # Q5: Healthcare visits (respiratory_Essence)
  #     ED + IP percentages by week
  # ================================================================
  cat("Q5: Healthcare visit percentages...\n")

  q5 <- build_payload(
    from_list = list(from_essence),
    select_list = list(
      col_ref("r", "startdate"),
      col_ref("r", "MMWRWKYR"),
      col_ref("r", "ED_ARI"),
      col_ref("r", "ED_COVID"),
      col_ref("r", "ED_FLU"),
      col_ref("r", "ED_RSV"),
      col_ref("r", "IP_ARI"),
      col_ref("r", "IP_COVID"),
      col_ref("r", "IP_FLU"),
      col_ref("r", "IP_RSV")
    )
  )
  q5_rj <- execute_query(q5)
  visits <- decode_dsr(q5_rj, c(
    "startdate", "mmwrwkyr",
    "ed_ari_pct", "ed_covid_pct",
    "ed_flu_pct", "ed_rsv_pct",
    "ip_ari_pct", "ip_covid_pct",
    "ip_flu_pct", "ip_rsv_pct"
  ))
  if (!is.null(visits)) {
    visits$startdate <- convert_pbi_date(visits$startdate)
    pct_cols <- grep("_pct$", names(visits), value = TRUE)
    for (col in pct_cols) {
      visits[[col]] <- round(as.numeric(visits[[col]]) * 100, 2)
    }
    cat("  ", nrow(visits), "rows\n")
    all_data[["healthcare_visits"]] <- visits
  }

  # ================================================================
  # Q6: Mortality by week + cause + season
  # ================================================================
  cat("Q6: Mortality data...\n")

  q6 <- build_payload(
    from_list = list(from_death),
    select_list = list(
      col_ref("d", "MMWRWKYR"),
      col_ref("d", "morb"),
      col_ref("d", "SEASON"),
      msr_ref("d", "Death Count")
    )
  )
  q6_rj <- execute_query(q6)
  mortality <- decode_dsr(q6_rj, c(
    "mmwrwkyr", "cause", "season", "death_count"
  ))
  if (!is.null(mortality)) {
    cat("  ", nrow(mortality), "rows\n")
    all_data[["mortality"]] <- mortality
  }

  # ================================================================
  # Combine all datasets into single standardized output
  # ================================================================

  if (length(all_data) == 0) stop("All Power BI queries returned empty results")

  # Tag each dataset with its source descriptor before combining
  for (nm in names(all_data)) {
    all_data[[nm]]$source_dataset <- nm
  }
  data_raw <- bind_rows(all_data)

  # Standardize column names
  names(data_raw) <- tolower(gsub("[^a-z0-9]", "_", names(data_raw)))

  # Save raw JSON responses for audit trail
  writeLines(toJSON(q1_rj, auto_unbox = TRUE), "raw/q1_cases_statewide.json")
  if (!is.null(q5_rj)) writeLines(toJSON(q5_rj, auto_unbox = TRUE), "raw/q5_healthcare_visits.json")
  if (!is.null(q6_rj)) writeLines(toJSON(q6_rj, auto_unbox = TRUE), "raw/q6_mortality.json")

  # Write single standardized output
  vroom::vroom_write(data_raw, "standard/data.csv.gz", delim = ",")
  process$success <- TRUE

  list(success = TRUE, rows = nrow(data_raw),
       message = paste("Power BI data:", nrow(data_raw), "rows from",
                       length(all_data), "query sets:",
                       paste(names(all_data), collapse = ", ")))

}, error = function(e) {
  list(success = FALSE, rows = 0L, message = conditionMessage(e))
})

# Save result
process$last_run <- as.character(Sys.time())
write(toJSON(process, auto_unbox = TRUE, pretty = TRUE), "process.json")
saveRDS(result, "process_result.rds")
cat(sprintf("[%s] success=%s rows=%d msg=%s\n",
    state_name, result$success, result$rows, result$message))
