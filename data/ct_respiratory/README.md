# Scraping state-level data identified by CMU Delphi 

## Summary
The Delphi group's Epiportal has URLs pointing to data from state health departments for a number of infectious disease indicators. This repository uses Claude to (1) create acsv file that pulls the URLS and other metadata from the epiportal (2) In plan mode with Claude Opus, iteratively tried the URLS to download the data

We use the data conventions from the PopHIVE ingest project and the dcf() package. 