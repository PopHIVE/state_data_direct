# coverage_report.R - reads existing results, compares to epiportal indicators
# Run from state_data_direct/

suppressPackageStartupMessages({
  library(dplyr)
  library(vroom)
  library(jsonlite)
})

# ---- 1. Load epiportal indicators ----
ind <- read.csv("epiportal_state_indicators.csv", stringsAsFactors=FALSE)
state_map <- c(Alabama="AL",Alaska="AK",Arizona="AZ",California="CA",Colorado="CO",
               Connecticut="CT",Florida="FL",Georgia="GA",Iowa="IA",Illinois="IL",
               Indiana="IN",Kentucky="KY",Louisiana="LA",Massachusetts="MA",
               Maryland="MD",Michigan="MI",Minnesota="MN",Missouri="MO",
               "New Jersey"="NJ","New York"="NY","North Carolina"="NC",Ohio="OH",
               Oklahoma="OK",Oregon="OR",Pennsylvania="PA","South Carolina"="SC",
               Tennessee="TN",Texas="TX",Utah="UT",Vermont="VT",Virginia="VA",
               Washington="WA",Wisconsin="WI")
ind$code <- state_map[ind$state]

# ---- 2. Collect all results ----
dirs <- list.dirs("data", recursive=FALSE, full.names=FALSE)
dirs <- dirs[grepl("_respiratory$", dirs)]

collect <- function(dir_name) {
  path  <- file.path("data", dir_name)
  code  <- toupper(sub("_respiratory$", "", dir_name))
  rds   <- file.path(path, "process_result.rds")
  std   <- file.path(path, "standard", "data.csv.gz")
  if (!file.exists(rds)) return(data.frame(code=code,success=FALSE,rows=0,msg="no result",cols="",vals="",stringsAsFactors=FALSE))
  r <- readRDS(rds)
  cols <- vals <- ""
  if (file.exists(std)) tryCatch({
    d <- vroom(std, show_col_types=FALSE, n_max=3)
    cols <- paste(tolower(names(d)), collapse=" ")
    vals <- paste(tolower(unlist(d[1:min(2,nrow(d)),])), collapse=" ")
  }, error=function(e) NULL)
  data.frame(code=code, success=isTRUE(r$success), rows=as.integer(r$rows),
             msg=as.character(r$message), cols=cols, vals=vals, stringsAsFactors=FALSE)
}

res <- do.call(rbind, lapply(dirs, collect))
row.names(res) <- NULL

# ---- 3. Pathogen keyword check ----
check_pathogen <- function(all_text, pathogen) {
  p <- tolower(pathogen)
  kws <- list(
    "covid" = c("covid","sars","2019"),
    "influenza" = c("influenza","flu","ili"),
    "rsv" = c("rsv","syncytial"),
    "ari" = c("ari","acute.respir"),
    "ili" = c("ili","influenza.like"),
    "respiratory" = c("respiratory","respir"),
    "wastewater" = c("wastewat","wval","concentrat")
  )
  for (k in names(kws)) {
    if (grepl(k, p)) return(any(sapply(kws[[k]], function(w) grepl(w, all_text))))
  }
  # fallback: any word > 4 chars from pathogen name in text
  words <- Filter(function(w) nchar(w)>4, strsplit(p,"[^a-z]")[[1]])
  if (length(words)==0) return(FALSE)
  any(sapply(words, function(w) grepl(w, all_text)))
}

# ---- 4. Generate report ----
sink("coverage_report.txt")
cat("================================================================\n")
cat("EPIPORTAL COVERAGE REPORT\n")
cat(sprintf("Generated: %s\n", Sys.time()))
cat("================================================================\n\n")

ok_states <- res$code[res$success]
fail_states <- res$code[!res$success]

cat(sprintf("States with data: %d/%d  |  Total rows: %s\n\n",
    length(ok_states), nrow(res), format(sum(res$rows), big.mark=",")))

# Summary table
cat("SCRAPING RESULTS\n")
cat(strrep("-",70),"\n")
for (i in seq_len(nrow(res))) {
  r <- res[i,]
  sym <- if (r$success) "OK  " else "FAIL"
  cat(sprintf("  %s %-3s  %8s rows  %s\n", sym, r$code,
              format(r$rows, big.mark=","), substr(r$msg, 1, 55)))
}

# Indicator coverage per state
cat("\n\nINDICATOR COVERAGE BY STATE\n")
cat(strrep("=",70),"\n\n")

state_summary <- data.frame(
  code=character(), name=character(), n_indicators=integer(),
  n_covered=integer(), missing_pathogens=character(),
  status=character(), stringsAsFactors=FALSE)

for (sc in sort(unique(ind$code))) {
  st_ind <- ind[!is.na(ind$code) & ind$code==sc, ]
  n_ind  <- nrow(st_ind)
  st_name <- st_ind$state[1]
  r      <- res[res$code==sc, ]
  if (nrow(r)==0 || !r$success) {
    pathogens <- paste(unique(st_ind$pathogens[st_ind$pathogens!=""]), collapse="; ")
    links <- unique(st_ind$documentation_link[st_ind$documentation_link!="" &
                    !grepl("none",st_ind$documentation_link,ignore.case=TRUE)])
    cat(sprintf("%-15s (%s)  %2d indicators  STATUS: FAILED\n", st_name, sc, n_ind))
    cat(sprintf("  Pathogens needed: %s\n", pathogens))
    if (length(links)>0) cat(sprintf("  Source: %s\n", links[1]))
    cat(sprintf("  MISSING: all %d indicators\n\n", n_ind))
    state_summary <- rbind(state_summary, data.frame(code=sc, name=st_name,
      n_indicators=n_ind, n_covered=0L, missing_pathogens=pathogens, status="FAILED",
      stringsAsFactors=FALSE))
    next
  }
  all_text <- paste(r$cols, r$vals)
  pathogens_needed <- unique(st_ind$pathogens[st_ind$pathogens!=""])
  covered <- sapply(pathogens_needed, function(p) check_pathogen(all_text, p))
  n_cov  <- sum(covered)
  n_miss <- sum(!covered)
  miss_p <- pathogens_needed[!covered]

  # Estimate indicator coverage: count indicators whose pathogen is covered
  ind_coverage <- sapply(seq_len(nrow(st_ind)), function(i) {
    p <- st_ind$pathogens[i]
    if (p=="") return(TRUE)
    check_pathogen(all_text, p)
  })
  n_ind_covered <- sum(ind_coverage)
  missing_inds  <- st_ind$name[!ind_coverage]

  pct <- round(100*n_ind_covered/n_ind)
  cat(sprintf("%-15s (%s)  %2d indicators  %2d/%2d covered (%d%%)  [%s rows]\n",
      st_name, sc, n_ind, n_ind_covered, n_ind, pct, format(r$rows, big.mark=",")))
  if (length(miss_p)>0) cat(sprintf("  Missing pathogens: %s\n", paste(miss_p,collapse="; ")))
  if (length(missing_inds)>0 && length(missing_inds)<=5) {
    cat(sprintf("  Missing indicators: %s\n", paste(missing_inds, collapse="; ")))
  } else if (length(missing_inds)>5) {
    cat(sprintf("  Missing indicators (%d): %s ...\n",
        length(missing_inds), paste(head(missing_inds,4), collapse="; ")))
  }
  cat("\n")
  state_summary <- rbind(state_summary, data.frame(code=sc, name=st_name,
    n_indicators=n_ind, n_covered=as.integer(n_ind_covered),
    missing_pathogens=paste(miss_p, collapse="; "),
    status=if(pct>=80)"GOOD" else if(pct>=40)"PARTIAL" else "POOR",
    stringsAsFactors=FALSE))
}

cat("\n\nSUMMARY TABLE\n")
cat(strrep("=",70),"\n")
cat(sprintf("%-15s %-5s %10s %10s %8s  %-10s\n",
    "State","Code","Indicators","Covered","Pct","Status"))
cat(strrep("-",70),"\n")
for (i in seq_len(nrow(state_summary))) {
  s <- state_summary[i,]
  pct_str <- if(s$n_covered>0) sprintf("%d%%", round(100*s$n_covered/s$n_indicators)) else "0%"
  cat(sprintf("%-15s %-5s %10d %10d %8s  %-10s\n",
      s$name, s$code, s$n_indicators, s$n_covered, pct_str, s$status))
}
cat(strrep("-",70),"\n")
cat(sprintf("%-15s %-5s %10d %10d\n", "TOTAL","",sum(state_summary$n_indicators),sum(state_summary$n_covered)))
sink()

# Also write CSV
write.csv(state_summary, "coverage_summary.csv", row.names=FALSE)
cat("Report written to coverage_report.txt and coverage_summary.csv\n")
cat(readLines("coverage_report.txt"), sep="\n")
