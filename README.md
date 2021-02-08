# BOM Observations Scraper

This is a short routine ran on a 24 hourly basis using the Python package *schedule*. A handful of observations are scraped from the BOM website, cleaned, then put into a Postgres database. This database is accessed and the data is used in the prediction of rainfall for tomorrow. 

Nothing special as the BOM website renders without Javascript enabled. Meaning the data can be extracted using the basic requests package. For my sanity I feed the page source into Pandas.read_html and let it parse out the usable tables. The dataframes are then manipulated to check if the 3pm observations are present and if they are, extract, clean and placed into the database. 