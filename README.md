# Iowa Liquor Sales Case Study
Steven Pisani
> The following answers are more thoroughly detailed with the output of my analysis notebook saved in the `3_data_analysis` directory.

## Table of Contents
1. [Data Ingestion](#data-ingestion)
2. [Data Transformation](#data-transformation)
3. [Data Analysis](#data-analysis)
4. [Technologies Used](#technologies-used)
5. [Getting Started](#getting-started)
6. [Notes on Scalability and Data Quality](#notes-on-scalability-and-data-quality)

# Prompt
In the [`0_prompt`](./0_prompt/) directory, you will find a markdown file with the prompt for this case study and a
link to download raw data from Google Drive.

# Data Ingestion
In the [`1_data_ingestion`](./1_data_ingestion/) directory, you will find a python script that will ingest the data from the Iowa Liquor Sales
after it has been downloaded locally and load it into a Snowflake database as well as a [Readme](./1_data_ingestion/README.md)
with additional details.

# Data Transformation
In the [`2_data_transformation`](./2_data_transformation/) directory, you will find a dbt project that will transform
the data from the previous step into a set of normalized tables that can be used for analysis. There is also 
a [Readme](./2_data_transformation/README.md) with additional details.

# Data Analysis
The `3_data_analysis` directory contains a detailed analysis of the transformed data, including:

- Impact of COVID-19 on the Iowa liquor market
- Trends in liquor sales over time
- Changes in market share for different types of liquor
- Top 10 retailers by year
- Analysis of Heaven Hill Brands' acquisition impact
- Identification and handling of data integrity issues

For full details and visualizations, please refer to the Jupyter notebook in this directory.

# Technologies Used
- Python for data ingestion and processing
- Snowflake for data warehousing
- dbt for data transformation
- Jupyter Notebooks for data analysis and visualization

# Getting Started
1. Clone this repository
2. Follow the instructions in the `1_data_ingestion/README.md` to set up your Snowflake account and ingest the data
3. Proceed to the `2_data_transformation/README.md` for guidance on running the dbt project
4. Explore the analysis results in the `3_data_analysis` directory

# Notes on Scalability and Data Quality
This project includes considerations for dealing with large datasets and addressing data quality issues. Some key points:

- The raw dataset is a denormalized fact table, which was normalized using dbt for easier analysis
- Window functions were used to handle slowly changing dimensions
- For production, it's recommended to use effective date ranges for dimension values
- A more scalable solution would involve expanding the current framework with more tests on both raw and modeled data
- Exploring better data ingestion endpoints could reduce the need for heavy normalization work

For more details on data quality issues and their solutions, refer to the analysis in the `3_data_analysis` directory.
