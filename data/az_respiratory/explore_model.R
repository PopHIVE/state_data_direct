library(httr2)
library(jsonlite)

# === Arizona Power BI Model Explorer ===
# Discovers all tables, columns, and measures available in the AZ respiratory dashboard

resource_key <- "733d649c-e71b-4cbc-a2ae-fb86d7e480d2"
report_id    <- "a573a223-c57f-498f-86a4-b35c27a62558"
dataset_id   <- "f3cd4838-0724-4753-b65f-0bec3f4a71f2"
model_id     <- 791111
base_api     <- "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

headers <- c(
  "Content-Type"         = "application/json;charset=UTF-8",
  "X-PowerBI-ResourceKey" = resource_key,
  "User-Agent"           = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
)

# ------------------------------------------------------------------
# 1. Try the modelsAndExploration endpoint (gives full schema)
# ------------------------------------------------------------------
cat("=== Trying modelsAndExploration endpoint ===\n")
schema_url <- paste0(base_api, "/public/reports/", report_id,
                     "/modelsAndExploration?preferReadOnlySession=true")

schema_resp <- tryCatch({
  request(schema_url) |>
    req_headers(!!!headers) |>
    req_error(is_error = \(resp) FALSE) |>
    req_perform()
}, error = function(e) { cat("Request failed:", e$message, "\n"); NULL })

if (!is.null(schema_resp)) {
  cat("Status:", resp_status(schema_resp), "\n")
  if (resp_status(schema_resp) == 200) {
    schema_json <- resp_body_json(schema_resp, check_type = FALSE)

    # Save raw schema for inspection
    writeLines(toJSON(schema_json, auto_unbox = TRUE, pretty = TRUE),
               "raw/model_schema.json")
    cat("Full schema saved to raw/model_schema.json\n")

    # Extract table/column info from the exploration model
    model <- schema_json$exploration$model
    if (!is.null(model$entities)) {
      cat("\n=== TABLES AND COLUMNS ===\n")
      for (entity in model$entities) {
        cat("\n--- Table:", entity$name, "---\n")
        if (!is.null(entity$columns)) {
          for (col in entity$columns) {
            cat("  Column:", col$name, " | type:", col$dataType %||% "?", "\n")
          }
        }
        if (!is.null(entity$measures)) {
          for (m in entity$measures) {
            cat("  Measure:", m$name, "\n")
            if (!is.null(m$expression)) cat("    expr:", m$expression, "\n")
          }
        }
      }
    }
  }
}

# ------------------------------------------------------------------
# 2. Also try the conceptualSchema endpoint
# ------------------------------------------------------------------
cat("\n=== Trying conceptualSchema endpoint ===\n")
cs_url <- paste0(base_api, "/public/reports/", report_id,
                 "/conceptualschema")
cs_resp <- tryCatch({
  request(cs_url) |>
    req_headers(!!!headers) |>
    req_error(is_error = \(resp) FALSE) |>
    req_perform()
}, error = function(e) { cat("Request failed:", e$message, "\n"); NULL })

if (!is.null(cs_resp)) {
  cat("Status:", resp_status(cs_resp), "\n")
  if (resp_status(cs_resp) == 200) {
    cs_json <- resp_body_json(cs_resp, check_type = FALSE)
    writeLines(toJSON(cs_json, auto_unbox = TRUE, pretty = TRUE),
               "raw/conceptual_schema.json")
    cat("Conceptual schema saved to raw/conceptual_schema.json\n")
  }
}

cat("\nDone. Check raw/model_schema.json for full details.\n")
