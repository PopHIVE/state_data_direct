library(httr2)
library(jsonlite)

url <- "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
headers <- c(
  "Content-Type" = "application/json;charset=UTF-8",
  "X-PowerBI-ResourceKey" = "733d649c-e71b-4cbc-a2ae-fb86d7e480d2",
  "User-Agent" = "Mozilla/5.0"
)

query_distinct <- function(entity_name, entity_alias,
                           column_name, label) {
  payload <- list(
    version = "1.0.0",
    queries = I(list(list(
      Query = list(Commands = I(list(list(
        SemanticQueryDataShapeCommand = list(
          Query = list(
            Version = 2,
            From = list(
              list(Name = entity_alias, Entity = entity_name,
                   Type = 0)
            ),
            Select = list(
              list(Column = list(
                Expression = list(
                  SourceRef = list(Source = entity_alias)
                ),
                Property = column_name
              ))
            )
          ),
          Binding = list(
            Primary = list(
              Groupings = I(list(
                list(Projections = I(list(0L)))
              ))
            ),
            DataReduction = list(
              DataVolume = 4,
              Primary = list(Top = list(Count = 5000))
            ),
            Version = 1
          )
        )
      )))),
      ApplicationContext = list(
        DatasetId = "f3cd4838-0724-4753-b65f-0bec3f4a71f2",
        Sources = I(list(list(
          ReportId = "a573a223-c57f-498f-86a4-b35c27a62558",
          VisualId = "1cd7ecf0e0d00b91ae70"
        )))
      )
    ))),
    modelId = 791111
  )

  resp <- request(url) |>
    req_headers(!!!headers) |>
    req_body_raw(toJSON(payload, auto_unbox = TRUE),
                 type = "application/json") |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()

  if (resp_status(resp) != 200) {
    cat(label, ": HTTP", resp_status(resp), "\n")
    return(NULL)
  }

  rj <- resp_body_json(resp, check_type = FALSE)

  # Save raw response for debugging first query
  if (label == "SEASONS") {
    writeLines(
      toJSON(rj, auto_unbox = TRUE, pretty = TRUE),
      "raw/probe_seasons_raw.json"
    )
  }

  # Check for value dictionaries (DSR ValueDicts)
  ds <- rj$results[[1]]$result$data$dsr$DS[[1]]
  dict <- ds$ValueDicts
  rows <- ds$PH[[1]]$DM0

  cat(label, ":", length(rows), "rows\n")

  # Show dict if present
  if (!is.null(dict)) {
    cat("  ValueDicts keys:", paste(names(dict), collapse = ", "),
        "\n")
    for (dk in names(dict)) {
      cat("  Dict[", dk, "]:",
          paste(head(dict[[dk]], 30), collapse = ", "), "\n")
    }
  }

  # Show first few raw rows
  cat("  First 3 rows:\n")
  for (i in seq_len(min(3, length(rows)))) {
    cat("   ", toJSON(rows[[i]], auto_unbox = TRUE), "\n")
  }
  cat("\n")
}

query_distinct("Table_County_Season", "t", "SEASON", "SEASONS")
query_distinct("Table_County_Season", "t", "COUNTYNM", "COUNTIES")
query_distinct(
  "Pub1ic respiratory_flu_case_data", "f", "AGEGP",
  "AGE GROUPS (flu)"
)
query_distinct(
  "Pub1ic respiratory_covid_case_data", "c", "AGEGP",
  "AGE GROUPS (covid)"
)
query_distinct(
  "Pub1ic respiratory_flu_case_data", "f", "TYPE",
  "FLU TYPES"
)
query_distinct(
  "Pub1ic respiratory_flu_case_data", "f", "SUBTYPE",
  "FLU SUBTYPES"
)
query_distinct(
  "Pub1ic respiratory_death_data", "d", "morb",
  "MORTALITY CAUSES"
)
query_distinct(
  "Parameter - ED Vs IP", "p", "Select ED Vs IPV",
  "VISIT TYPES"
)
