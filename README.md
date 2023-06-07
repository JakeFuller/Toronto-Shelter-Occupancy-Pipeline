# Toronto Shelter Occupancy Pipeline Project

## Overview

This is an ongoing End-To-End Pipeline project that will provide the most recent data and insights on Toronto shelter occupancy. This is an educational project that will be used to learn about End-To-End ETL pipelines and develop data analysis skills using:

* Python and Pandas
* BigQuery
* Tableau
* CRON Jobs

Data is fetched daily from a Toronto Open Data API using Python with Pandas. The data collected from the API is stored on local disk before being moved into BigQuery for aggregation, exploration and analysis. Finally, visualizations are created using Tableau to convey important information about the current state of Toronto's shelter occupancy. 