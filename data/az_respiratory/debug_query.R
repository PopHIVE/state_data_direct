library(httr2)
library(jsonlite)

api_url <- paste0(
  "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net",
  "/public/reports/querydata?synchronous=true"
)
headers <- c(
  "Content-Type" = "application/json;charset=UTF-8",
  "X-PowerBI-ResourceKey" =
    "733d649c-e71b-4cbc-a2ae-fb86d7e480d2",
  "User-Agent" = "Mozilla/5.0"
)
dataset_id <- "f3cd4838-0724-4753-b65f-0bec3f4a71f2"
report_id <- "a573a223-c57f-498f-86a4-b35c27a62558"

do_query <- function(payload, label) {
  resp <- request(api_url) |>
    req_headers(!!!headers) |>
    req_body_raw(
      toJSON(payload, auto_unbox = TRUE),
      type = "application/json"
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  cat(label, "- Status:", resp_status(resp), "\n")
  if (resp_status(resp) == 200) {
    rj <- resp_body_json(resp, check_type = FALSE)
    ds <- rj$results[[1]]$result$data$dsr$DS[[1]]
    rows <- ds$PH[[1]]$DM0
    cat("  Rows:", length(rows), "\n")
    if (!is.null(ds$ValueDicts)) {
      for (k in names(ds$ValueDicts)) {
        cat("  Dict[", k, "]:",
            paste(head(ds$ValueDicts[[k]], 8),
                  collapse = ", "),
            "\n")
      }
    }
    for (i in seq_len(min(3, length(rows)))) {
      cat("  ",
          toJSON(rows[[i]], auto_unbox = TRUE),
          "\n")
    }
  } else {
    cat("  Error:",
        substr(resp_body_string(resp), 1, 300),
        "\n")
  }
  cat("\n")
}

# Test A: Add SEASON as a grouping column
# (mimics the Summary page with season slicer)
cat("=== TEST A: Cases + SEASON column ===\n")
do_query(list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "p",
                 Entity =
                   "Pub1ic respiratory_flu_case_data",
                 Type = 0),
            list(Name = "t",
                 Entity = "Table_Startdate",
                 Type = 0),
            list(Name = "tc",
                 Entity = "Table_County_Season",
                 Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "tc")
              ),
              Property = "SEASON"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "t")
              ),
              Property = "startdate"
            )),
            list(Measure = list(
              Expression = list(
                SourceRef = list(Source = "p")
              ),
              Property = "Flu DC MEDSISID"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(list(
              Projections = c(0L, 1L, 2L)
            )))
          ),
          DataReduction = list(
            DataVolume = 4,
            Primary = list(
              Top = list(Count = 30000)
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
        VisualId = "1cd7ecf0e0d00b91ae70"
      )))
    )
  ))),
  modelId = 791111
), "Season+Date+FluCount")

# Test B: Add COUNTYNM as a grouping column
cat("=== TEST B: Cases + COUNTY column ===\n")
do_query(list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "p",
                 Entity =
                   "Pub1ic respiratory_flu_case_data",
                 Type = 0),
            list(Name = "t",
                 Entity = "Table_Startdate",
                 Type = 0),
            list(Name = "tc",
                 Entity = "Table_County_Season",
                 Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "tc")
              ),
              Property = "COUNTYNM"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "t")
              ),
              Property = "startdate"
            )),
            list(Measure = list(
              Expression = list(
                SourceRef = list(Source = "p")
              ),
              Property = "Flu DC MEDSISID"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(list(
              Projections = c(0L, 1L, 2L)
            )))
          ),
          DataReduction = list(
            DataVolume = 4,
            Primary = list(
              Top = list(Count = 30000)
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
        VisualId = "1cd7ecf0e0d00b91ae70"
      )))
    )
  ))),
  modelId = 791111
), "County+Date+FluCount")

# Test C: AGEGP from the flu table directly
cat("=== TEST C: Cases + AGEGP (flu table) ===\n")
do_query(list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "p",
                 Entity =
                   "Pub1ic respiratory_flu_case_data",
                 Type = 0),
            list(Name = "t",
                 Entity = "Table_Startdate",
                 Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "p")
              ),
              Property = "AGEGP"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "t")
              ),
              Property = "startdate"
            )),
            list(Measure = list(
              Expression = list(
                SourceRef = list(Source = "p")
              ),
              Property = "Flu DC MEDSISID"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(list(
              Projections = c(0L, 1L, 2L)
            )))
          ),
          DataReduction = list(
            DataVolume = 4,
            Primary = list(
              Top = list(Count = 30000)
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
        VisualId = "1cd7ecf0e0d00b91ae70"
      )))
    )
  ))),
  modelId = 791111
), "AgeGp+Date+FluCount")

# Test D: Healthcare visits (respiratory_Essence)
# directly querying columns instead of measures
cat("=== TEST D: Healthcare visits raw ===\n")
do_query(list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "r",
                 Entity = "respiratory_Essence",
                 Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "startdate"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "ED_ARI"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "ED_FLU"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "ED_COVID"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "ED_RSV"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "IP_ARI"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "IP_FLU"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "IP_COVID"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "r")
              ),
              Property = "IP_RSV"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(list(
              Projections = c(
                0L, 1L, 2L, 3L, 4L,
                5L, 6L, 7L, 8L
              )
            )))
          ),
          DataReduction = list(
            DataVolume = 4,
            Primary = list(
              Top = list(Count = 30000)
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
        VisualId = "1cd7ecf0e0d00b91ae70"
      )))
    )
  ))),
  modelId = 791111
), "Essence raw cols")

# Test E: Mortality
cat("=== TEST E: Mortality ===\n")
do_query(list(
  version = "1.0.0",
  queries = I(list(list(
    Query = list(Commands = I(list(list(
      SemanticQueryDataShapeCommand = list(
        Query = list(
          Version = 2,
          From = list(
            list(Name = "d",
                 Entity =
                   "Pub1ic respiratory_death_data",
                 Type = 0)
          ),
          Select = list(
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "d")
              ),
              Property = "MMWRWKYR"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "d")
              ),
              Property = "morb"
            )),
            list(Column = list(
              Expression = list(
                SourceRef = list(Source = "d")
              ),
              Property = "SEASON"
            )),
            list(Measure = list(
              Expression = list(
                SourceRef = list(Source = "d")
              ),
              Property = "Death Count"
            ))
          )
        ),
        Binding = list(
          Primary = list(
            Groupings = I(list(list(
              Projections = c(0L, 1L, 2L, 3L)
            )))
          ),
          DataReduction = list(
            DataVolume = 4,
            Primary = list(
              Top = list(Count = 30000)
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
        VisualId = "1cd7ecf0e0d00b91ae70"
      )))
    )
  ))),
  modelId = 791111
), "Mortality by week/cause/season")
