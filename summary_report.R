# summary_report.R
# Runs all state ingest scripts and generates a scraping summary report.
# Run from state_data_direct/ directory.

library(dplyr)
library(vroom)

BASE <- getwd()

# Discover all state source directories
state_dirs <- list.dirs("data", recursive=FALSE, full.names=FALSE)
state_dirs <- state_dirs[grepl("_respiratory$", state_dirs)]

cat(sprintf("Found %d state projects to process.\n\n", length(state_dirs)))

# ---------------------------------------------------------------------------
# Run each ingest script and collect results
# ---------------------------------------------------------------------------
results <- lapply(state_dirs, function(dir_name) {
  dir_path    <- file.path("data", dir_name)
  result_file <- file.path(dir_path, "process_result.rds")
  standard_f  <- file.path(dir_path, "standard", "data.csv.gz")

  state_code <- sub("_respiratory$", "", dir_name)
  cat(sprintf("[%s] Running ingest...", toupper(state_code)))

  # Change to state directory and source ingest.R
  old_wd <- setwd(dir_path)
  tryCatch({
    source("ingest.R", local=TRUE)
  }, error=function(e) {
    cat(sprintf(" ERROR during source: %s\n", conditionMessage(e)))
    saveRDS(list(success=FALSE, rows=0L, message=conditionMessage(e)), "process_result.rds")
  })
  setwd(old_wd)

  # Read result
  if (file.exists(result_file)) {
    r <- readRDS(result_file)
    cat(sprintf(" %s (%d rows)\n", if(r$success) "SUCCESS" else "FAILED", r$rows))
    tibble(
      state      = toupper(state_code),
      dir        = dir_name,
      success    = r$success,
      rows       = as.integer(r$rows),
      message    = as.character(r$message),
      has_output = file.exists(standard_f)
    )
  } else {
    cat(" NO RESULT FILE\n")
    tibble(state=toupper(state_code), dir=dir_name, success=FALSE,
           rows=0L, message="No process_result.rds created", has_output=FALSE)
  }
})

results_df <- bind_rows(results)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
n_total   <- nrow(results_df)
n_success <- sum(results_df$success)
n_failed  <- n_total - n_success

cat("\n")
cat("========================================\n")
cat("         SCRAPING SUMMARY REPORT        \n")
cat("========================================\n")
cat(sprintf("Total states:   %d\n", n_total))
cat(sprintf("Succeeded:      %d (%.0f%%)\n", n_success, 100*n_success/n_total))
cat(sprintf("Failed:         %d (%.0f%%)\n", n_failed,  100*n_failed/n_total))
cat("\n")

if (n_success > 0) {
  cat("--- SUCCEEDED ---\n")
  results_df |>
    filter(success) |>
    select(state, rows, message) |>
    as.data.frame() |>
    print(row.names=FALSE)
  cat("\n")
}

cat("--- FAILED ---\n")
results_df |>
  filter(!success) |>
  select(state, message) |>
  as.data.frame() |>
  print(row.names=FALSE)

# ---------------------------------------------------------------------------
# Failure category analysis
# ---------------------------------------------------------------------------
cat("\n--- FAILURE CATEGORIES ---\n")
failed_df <- results_df |> filter(!success)
if (nrow(failed_df) > 0) {
  failed_df <- failed_df |>
    mutate(category = case_when(
      grepl("PowerBI|Tableau|JavaScript dashboard", message) ~ "Dashboard-only (no API)",
      grepl("No CSV|No downloadable|No .* links found", message) ~ "No download links found",
      grepl("HTTP 4|HTTP 5|status_code", message)               ~ "HTTP error",
      grepl("timed? ?out|timeout|TIMEOUT", message, ignore.case=TRUE) ~ "Timeout",
      grepl("dataset ID unknown|not accessible", message)        ~ "Dataset ID not found",
      TRUE ~ "Other error"
    ))
  print(table(failed_df$category))
}

# ---------------------------------------------------------------------------
# Write CSV report
# ---------------------------------------------------------------------------
report_file <- "scraping_summary_report.csv"
write.csv(results_df, report_file, row.names=FALSE)
cat(sprintf("\nFull report saved to: %s\n", report_file))

# ---------------------------------------------------------------------------
# Write Markdown report with per-indicator documentation
# ---------------------------------------------------------------------------
md_file <- "scraping_summary_report.md"
md_lines <- c(
  "# State Respiratory Data Scraping Report",
  "",
  sprintf("**Date:** %s", Sys.Date()),
  sprintf("**Total states:** %d | **Succeeded:** %d (%0.f%%) | **Failed:** %d (%0.f%%)",
    n_total, n_success, 100*n_success/n_total, n_failed, 100*n_failed/n_total),
  ""
)

# Summary table
md_lines <- c(md_lines,
  "## Summary by State",
  "",
  "| State | Status | Rows | Message |",
  "|-------|--------|------|---------|"
)
for (i in seq_len(nrow(results_df))) {
  status <- if (results_df$success[i]) "SUCCESS" else "FAILED"
  md_lines <- c(md_lines, sprintf("| %s | %s | %s | %s |",
    results_df$state[i], status,
    format(results_df$rows[i], big.mark=","),
    results_df$message[i]))
}
md_lines <- c(md_lines, "")

# Per-indicator detail from epiportal CSV
epi_file <- "epiportal_state_indicators.csv"
if (file.exists(epi_file)) {
  epi <- tryCatch(vroom::vroom(epi_file, show_col_types=FALSE), error=function(e) NULL)
  if (!is.null(epi) && nrow(epi) > 0) {
    n_indicators <- nrow(epi)
    md_lines <- c(md_lines,
      sprintf("## Detailed Results by State (%d indicators)", n_indicators),
      ""
    )

    # Get unique states from epi
    epi_states <- sort(unique(epi$state))

    for (st in epi_states) {
      st_indicators <- epi[epi$state == st, ]
      n_ind <- nrow(st_indicators)

      # Look up scraping result
      st_upper <- toupper(substr(st, 1, 2))
      # Match by state name
      st_result <- results_df[toupper(sub("_respiratory$", "", results_df$dir)) ==
        tolower(substr(gsub("[^A-Za-z]", "", st), 1, 2)), ]
      # Fallback: match by state name in results
      if (nrow(st_result) == 0) {
        # Try matching abbreviation from dir name
        for (j in seq_len(nrow(results_df))) {
          dir_code <- sub("_respiratory$", "", results_df$dir[j])
          if (nrow(st_result) == 0) {
            st_result <- results_df[j, , drop=FALSE]
            # Reset if not matching
            st_result <- st_result[0, ]
          }
        }
      }

      # Simpler matching: use state name mapping
      state_abbrevs <- c(
        "Alabama"="al","Alaska"="ak","Arizona"="az","California"="ca",
        "Colorado"="co","Connecticut"="ct","Florida"="fl","Georgia"="ga",
        "Iowa"="ia","Illinois"="il","Indiana"="in","Kentucky"="ky",
        "Louisiana"="la","Massachusetts"="ma","Maryland"="md","Michigan"="mi",
        "Minnesota"="mn","Missouri"="mo","New Jersey"="nj","New York"="ny",
        "North Carolina"="nc","Ohio"="oh","Oklahoma"="ok","Oregon"="or",
        "Pennsylvania"="pa","South Carolina"="sc","Tennessee"="tn","Texas"="tx",
        "Utah"="ut","Vermont"="vt","Virginia"="va","Washington"="wa","Wisconsin"="wi"
      )
      st_code <- state_abbrevs[st]
      if (is.na(st_code)) st_code <- tolower(substr(st, 1, 2))
      st_dir <- paste0(st_code, "_respiratory")
      st_result <- results_df[results_df$dir == st_dir, ]

      if (nrow(st_result) > 0) {
        status_label <- if (st_result$success[1]) "SUCCESS" else "FAILED"
        rows_label   <- st_result$rows[1]
        msg_label    <- st_result$message[1]
      } else {
        status_label <- "NOT FOUND"
        rows_label   <- 0
        msg_label    <- "No matching scraper directory"
      }

      md_lines <- c(md_lines,
        sprintf("### %s (%s) — %s", st, toupper(st_code), status_label),
        sprintf("**Status:** %s | **Rows:** %s | **Indicators in CSV:** %d",
          status_label, format(rows_label, big.mark=","), n_ind),
        sprintf("**Reason:** %s", msg_label),
        ""
      )

      # List unique URLs for this state
      st_urls <- unique(st_indicators$documentation_link)
      st_urls <- st_urls[!is.na(st_urls) & st_urls != ""]
      if (length(st_urls) > 0) {
        md_lines <- c(md_lines, "**URLs tried:**")
        for (u in st_urls) {
          md_lines <- c(md_lines, sprintf("- %s", u))
        }
        md_lines <- c(md_lines, "")
      }

      # Indicator table
      md_lines <- c(md_lines,
        "| Indicator | Pathogen | Data Type | Status | Reason |",
        "|-----------|----------|-----------|--------|--------|"
      )

      for (k in seq_len(n_ind)) {
        ind_name <- st_indicators$name[k]
        pathogen <- if (!is.na(st_indicators$pathogens[k])) st_indicators$pathogens[k] else ""
        data_type <- if (!is.na(st_indicators$data_type[k])) st_indicators$data_type[k] else ""

        ind_status <- if (st_result$success[1] %in% TRUE) "imported" else "not imported"
        ind_reason <- if (st_result$success[1] %in% TRUE) {
          sprintf("Included in %s-row download", format(rows_label, big.mark=","))
        } else {
          msg_label
        }

        # Truncate long strings for table readability
        if (nchar(ind_name) > 60) ind_name <- paste0(substr(ind_name, 1, 57), "...")
        if (nchar(ind_reason) > 80) ind_reason <- paste0(substr(ind_reason, 1, 77), "...")

        md_lines <- c(md_lines, sprintf("| %s | %s | %s | %s | %s |",
          ind_name, pathogen, data_type, ind_status, ind_reason))
      }

      md_lines <- c(md_lines, "")
    }
  }
} else {
  md_lines <- c(md_lines, "*epiportal_state_indicators.csv not found — per-indicator detail skipped.*", "")
}

writeLines(md_lines, md_file)
cat(sprintf("Markdown report saved to: %s\n", md_file))

# ---------------------------------------------------------------------------
# Data quality check for successes
# ---------------------------------------------------------------------------
if (n_success > 0) {
  cat("\n--- OUTPUT DATA QUALITY ---\n")
  success_dirs <- results_df |> filter(success) |> pull(dir)
  for (d in success_dirs) {
    f <- file.path("data", d, "standard", "data.csv.gz")
    if (file.exists(f)) {
      tryCatch({
        dat <- vroom::vroom(f, show_col_types=FALSE, n_max=5)
        cat(sprintf("%s: %d cols - %s\n", d, ncol(dat), paste(names(dat), collapse=", ")))
      }, error=function(e) cat(sprintf("%s: could not read output\n", d)))
    }
  }
}

invisible(results_df)
