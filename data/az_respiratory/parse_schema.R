library(jsonlite)
schema <- fromJSON("raw/model_schema.json", simplifyVector = FALSE)

# Explore sections (report pages)
sections <- schema$exploration$sections
cat("=== REPORT PAGES ===\n")
for (s in sections) {
  cat("\nPage:", s$displayName, "\n")
  if (!is.null(s$visualContainers)) {
    cat("  Visuals:", length(s$visualContainers), "\n")
    for (vc in s$visualContainers) {
      cfg <- tryCatch(
        fromJSON(vc$config, simplifyVector = FALSE),
        error = function(e) NULL
      )
      if (is.null(cfg)) next
      vtype <- cfg$singleVisual$visualType
      if (is.null(vtype)) vtype <- "?"
      cat("  -", vtype, "\n")

      # Extract data roles / projections to find columns
      projs <- cfg$singleVisual$projections
      if (!is.null(projs)) {
        for (role_name in names(projs)) {
          for (proj in projs[[role_name]]) {
            qref <- proj$queryRef
            if (!is.null(qref)) {
              cat("    role:", role_name, "=>", qref, "\n")
            }
          }
        }
      }

      # Check prototypeQuery for From entities
      pq <- cfg$singleVisual$prototypeQuery
      if (!is.null(pq) && !is.null(pq$From)) {
        for (f in pq$From) {
          cat("    from:", f$Name, "->", f$Entity, "\n")
        }
      }
    }
  }
}
