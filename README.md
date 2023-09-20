# Iowa Liquor Sales Case Study
Steven Pisani
> The following answers are more thoroughly detailed with the output of my analysis notebook saved in the `3_data_analysis` directory.

## Table of Contents
1. [Data Ingestion](#data-ingestion)
2. [Data Transformation](#data-transformation)
3. [Questions](#questions)

# Data Ingestion
In the [`1_data_ingestion`](./1_data_ingestion/) directory, you will find a python script that will ingest the data from the Iowa Liquor Sales
after it has been downloaded locally and load it into a Snowflake database as well as a [Readme](./1_data_ingestion/README.md)
with additional details.

# Data Transformation
In the [`2_data_transformation`](./2_data_transformation/) directory, you will find a dbt project that will transform
the data from the previous step into a set of normalized tables that can be used for analysis. There is also 
a [Readme](./2_data_transformation/README.md) with additional details.

# Questions
## 1. What impact did Covid have on the overall liquor market in Iowa?
1. 2019 (Pre-COVID):
  Sales: $349,219,300
  Volume Sold: 22,301,328 liters
  
2. 2020 (COVID onset):
  Sales: $396,662,000 (an increase of about 12.7% from 2019)
  Volume Sold: 24,211,592 liters (an increase of about 8.2% from 2019)
  
3. 2021 (Post-COVID)
  Sales: $428,121,600 (an increase of about 7.6% from 2020)
  Volume Sold: 24,755,258 liters (an increase of about 2.2% from 2020)

From these numbers, we can infer:
The liquor market in Iowa saw growth both in terms of sales and volume during the COVID-19 period.
The year 2020, which marks the onset of COVID, saw an increase in sales and volume compared to 2019.
This growth trend continued into 2021, with even higher sales and volume compared to 2020.

###  a. What trends evolved over the next 3-18 months?
- Sales began to rise starting in January 2020 and peaked around March.
- There was a noticeable dip in April, likely reflecting the immediate impacts of COVID-19 lockdowns and restrictions.
- Post-April, sales began to recover steadily and maintained an upward trajectory throughout the year.
- By January 2021, sales had surged again and remained relatively high for the first half of the year, with a few fluctuations.

### b. Was there a notable shift in the types of products purchased in terms of pack size
- The most popular pack sizes in both years were 6, 12, and 24 bottles per pack.
- In 2020, there was a noticeable increase in purchases of packs containing 6 and 12 bottles compared to 2019.
- On the contrary, packs with 24 bottles saw a slight decrease in 2020 compared to 2019.

## 2. Which are the fastest growing types of liquor (e.g., vodka, tequila, rum, etc.)? How has market share changed over time?
It's evident that **Tequilas & Mezcal** experienced the highest growth, followed by Vodkas.

### a. Write a function that takes a list of liquor types as an input and visualizes the market share over time for each of those.
See `3_data_analysis/Iowa Liquor Sales Analysis.ipynb` for the function and output.
Comments from the output:
- **Tequilas & Mezcal**: A clear upward trend in market share, indicating its increasing popularity.
- **Vodkas**: A downward trend, suggesting customers are choosing other options.
- **Rums**: A relatively stable market share over the three years.

### b. What is driving the growth in tequila sales? Increases in average price or increases in volume sold?
1. **Volume Sold**:
There has been a consistent increase in the volume of Tequilas & Mezcal sold each year.

2. **Average Price per Liter**:
The average price per liter has also been increasing.
From 2019 to 2021, the average price per liter increased from approximately $24.94 to $27.63.

From this analysis, it's evident that the growth in Tequila sales is driven by both an increase in volume sold and an increase in the average price. Both factors have contributed to the overall growth in Tequila sales during this period.


## 3. Grouping individual store brands together (e.g., all of Walmart, Liquor Barn, Hy-Vee, etc.), who are the top 10 retailers by year?
| YEAR |                  2017 |                  2018 |                  2019 |                  2020 |                  2021 |
|-----:|----------------------:|----------------------:|----------------------:|----------------------:|----------------------:|
| RANK |                       |                       |                       |                       |                       |
|  1.0 |                Hy-Vee |                Hy-Vee |                Hy-Vee |                Hy-Vee |                Hy-Vee |
|  2.0 |        Fareway Stores |        Fareway Stores |        Fareway Stores |        Fareway Stores |        Fareway Stores |
|  3.0 |              Wal-Mart |              Wal-Mart |              Wal-Mart |              Wal-Mart |            Sam's Club |
|  4.0 |            Sam's Club |            Sam's Club |            Sam's Club |            Sam's Club | Casey's General Store |
|  5.0 |          Central City |          Central City |          Central City | Casey's General Store |              Wal-Mart |
|  6.0 | Casey's General Store | Casey's General Store | Casey's General Store |      Costco Wholesale |          Central City |
|  7.0 |              Kum & Go |              Kum & Go |              Kum & Go |          Central City |      Costco Wholesale |
|  8.0 |      Costco Wholesale |      Costco Wholesale |      Costco Wholesale |              Kum & Go |              Kum & Go |
|  9.0 |                   NaN |        Wilkie Liquors |        Wilkie Liquors |                   NaN |     Benz Distributing |
| 10.0 |             Walgreens |             Walgreens |         Lot-A-Spirits |             Walgreens |        Wilkie Liquors |

## 4. In late 2019 Heaven Hill Brands bought a portfolio of liquor brands from Constellation Brands. What percentage of Heaven Hill’s growth in 2020 can be attributed to the acquisition?
Sales of the acquired portfolio under Heaven Hill Brands in 2020: _$17,291,774.49_
Percentage of Heaven Hill Brands' growth in 2020 attributed to the acquisition: _Approximately 106.98%_

It's interesting to note that the growth attributed to the acquired portfolio is greater than 100%. This suggests that while Heaven Hill Brands experienced overall growth due to the acquired items, other parts of their portfolio might have seen a decline in sales.


## 5. What data integrity issues did you discover? How could you (or how did you) solve/account for these?
In its raw form, the dataset is a denormalized fact table with information on all entities involved in a transaction such as item, store, and vendor. Each of these entities also has a set of dimensions that would slowly change over time which would make analyses over long periods of time challenging. Additionally, some useful dimensions were not present in the raw dataset such as Store Brand.

To solve this, I first ingest the data into Snowflake and used dbt to normalize and clean the data before rejoining for analysis in this notebook.

### a. Comment on any data issues you discovered and what assumptions you used to deal with them.
As mentioned above, some dimensions in the entities such as which names, ids and associations changed over time in the raw dataset. To deal with these issues, I used a window function to take the latest values for a particular primary key.

In a production setting, I would suggest creating an effective from / to field on each primary key to value in each entity and joining to a date spine for quick analysis when needed.

### b. What’s a simple solution to solving data quality issues?
A simple solution may have been to load the data into a dataframe in the notebook directly and clean each column separately.

For some missing fields and hotfixes (_such as the brand name mappings_), I did so in this notebook.

### c. What would be a more scalable solution to deal with quality issues?
I would expand upon the framework I have created, adding more tests on the modeled data and the raw data as it is ingested. I would also suggest looking for better endpoints to ingest the raw data from to avoid having to do the heavy normalization work.
