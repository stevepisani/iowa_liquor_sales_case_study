# Data Ingestion

To get our data into Snowflake, we need to perform several preparation steps:

## 1. Setup a Snowflake account
Luckily, I already have a Snowflake account setup, so I will be using that!

## 2. Setup a Stage within Snowflake
A stage is a location where Snowflake can read and write data. We will be using a stage that is internal to Snowflake. In a production system, an external stage in a cloud provider such as AWS might be a more suitable choice.

## 3. Prepare the CSV for upload
Given the size of our CSV, we will be implementing a two-fold strategy:

### 3.1. Compressing the CSV
We will use the gzip compression format to reduce the size of our CSV. This step is beneficial even for smaller files, as it ensures reduced upload times and storage costs.

### 3.2. Splitting large CSV into manageable chunks
If our CSV is too large even after compression, we will split it into smaller, more manageable chunks. We leverage Python's inbuilt mechanisms to divide the file into parts, each of which is compressed separately before uploading.

## 4. Connect to Snowflake and test the connection
Before proceeding with the data upload, we will test our connection to Snowflake to ensure everything is set up correctly.

## 5. Upload the compressed (and possibly split) CSV to Snowflake
We will be using the Snowflake Python connector to load our data into Snowflake. This involves either uploading the compressed CSV or, if the file is vast, uploading the split and compressed chunks sequentially to Snowflake's internal stage. 

More details on the Snowflake Python connector can be found [here](https://docs.snowflake.com/en/user-guide/python-connector.html).

## 6. Verify the data has been loaded correctly
After loading our data into Snowflake, we need to verify its integrity. I find it best to do this in Snowflake's web UI, as it allows for easy querying and visualization of the data.

## 7. Load the data into a table from the stage
Once we have verified the data's integrity, we can load it into a table from the stage. This step is necessary as Snowflake does not allow us to query directly from the stage.
```sql
create file format standard_csv_w_header_quotes_unenclosed
    type = csv
    skip_header = 1
    field_optionally_enclosed_by='"'
    escape_unenclosed_field=none
;
copy into ingest_database.iowa_liquor_sales.raw_data
  from '@"INGEST_DATABASE"."IOWA_LIQUOR_SALES"."STAGE__IOWA_LIQUOR_SALES"'
  file_format = (format_name = ingest_database.iowa_liquor_sales.standard_csv_w_header_quotes_unenclosed)
  on_error = 'continue';
;
remove '@"INGEST_DATABASE"."IOWA_LIQUOR_SALES"."STAGE__IOWA_LIQUOR_SALES"' pattern='.*.csv.gz';
```
