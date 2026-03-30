library(httr2)
library(jsonlite)

url <- "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
headers <- c(
  "Content-Type" = "application/json;charset=UTF-8",
  "X-PowerBI-ResourceKey" = "733d649c-e71b-4cbc-a2ae-fb86d7e480d2",
  "User-Agent" = "Mozilla/5.0"
)

# Query flu case counts grouped by AGEGP + SEASON to see labels
payload <- list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "f", Entity = "Pub1ic respiratory_flu_case_data", Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(SourceRef = list(Source = "f")),
              Property = "AGEGP"
            )),
            list(Column = list(
              Expression = list(SourceRef = list(Source = "f")),
              Property = "SEASON"
            )),
            list(Measure = list(
              Expression = list(SourceRef = list(Source = "f")),
              Property = "Flu DC MEDSISID"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(
              list(Projections = I(list(0L, 1L, 2L)))
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
  req_perform()

rj <- resp_body_json(resp, check_type = FALSE)
writeLines(toJSON(rj, auto_unbox = TRUE, pretty = TRUE),
           "raw/probe_flu_age_season.json")

ds <- rj$results[[1]]$result$data$dsr$DS[[1]]
rows <- ds$PH[[1]]$DM0

cat("ValueDicts:\n")
if (!is.null(ds$ValueDicts)) {
  for (k in names(ds$ValueDicts)) {
    cat("  ", k, ":", paste(ds$ValueDicts[[k]], collapse = ", "), "\n")
  }
}

cat("\nFirst 10 rows:\n")
for (i in seq_len(min(10, length(rows)))) {
  cat("  ", toJSON(rows[[i]], auto_unbox = TRUE), "\n")
}
cat("\nTotal rows:", length(rows), "\n")
