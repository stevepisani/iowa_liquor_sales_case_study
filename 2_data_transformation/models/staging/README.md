# Staging
## Purpose of this subdirectory
The files in this dir are meant to capture all individual components we will be using to build our downstream models.

## Organization
The subdirectories here represent the different sources which our data is coming from.

There are 

In this case, the only source is our python ingestion script however the structure would extend to other sources as well.

### Python Ingestion Script
This is the script located in the data_ingestion subdirectory. It is responsible for taking the raw data from the Iowa Liquor Sales, compressing it, and uploading it to Snowflake.
