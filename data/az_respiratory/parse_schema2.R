library(jsonlite)
cs <- fromJSON("raw/conceptual_schema.json", simplifyVector = FALSE)

entities <- cs$schemas[[1]]$schema$Entities

for (entity in entities) {
  if (isTRUE(entity$Private)) next
  cat("\n--- Entity:", entity$Name, "---\n")
  if (!is.null(entity$Properties)) {
    for (prop in entity$Properties) {
      kind <- if (!is.null(prop$Column)) {
        "Col"
      } else if (!is.null(prop$Measure)) {
        "Msr"
      } else {
        "?"
      }
      hidden <- if (isTRUE(prop$Hidden)) " [hidden]" else ""
      cat("  ", kind, ":", prop$Name, hidden, "\n")
    }
  }
}
