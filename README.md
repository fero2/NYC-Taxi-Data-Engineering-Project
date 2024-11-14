# NYC Yellow Taxi Tripdata Analytics | Microsoft Azure Data Engineering Project 
Documentation of my Azure Data engineering project on the New York City Taxi trip dataset.

## Project Overview:

This project is a comprehensive data engineering solution using Microsoft Azure services, designed to process, transform, analyze, and visualize the New York City Yellow Taxi Trip data. The goal of this project was to create a scalable and efficient data pipeline that ingests raw taxi trip data, applies necessary transformations, loads it into a data warehouse, and generates insightful analytics through interactive dashboards.

## Project Architecture:
![Architecture Diagram](https://github.com/fero2/NYC-Taxi-Data-Engineering-Project/blob/main/NYC%20Taxi%20Project%20Overview%20-%20Architecture%20Diagram.jpg)

## Azure Services Used:

- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks
- Azure Synapse Analytics
- Key Vault
- Azure Active Directory
- Power BI

## Languages Used:
- **Programming language:** Python, Pyspark
- **Scripting language:** SQL

## Dataset:
NYC Yellow trip records include fields capturing pick-up and drop-off dates and times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

Here is the dataset used in this project: https://github.com/fero2/NYC-Taxi-Data-Engineering-Project/tree/main/Dataset

### More Info About Dataset:
1. Original data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf


## Project Implementation:
Lets understand how the specific azure resources were used here to implement the data engineering scope on this NYC yellow taxi trip dataset:

### 1. Azure Data Factory (ADF):
Orchestrated the entire ETL process, automating the data ingestion from external source to ADLS Gen2, triggering Databricks transformations and loading the transformed data to ADLS Gen2 again, and then loading into Synapse Analytics.

### 2. Azure Data Lake Storage Gen2 (ADLS Gen2): 
Served as the primary storage layer for the raw and transformed data. Data is ingested here initially and subsequently processed by other services.

### 3. Azure Databricks:
Performed data cleaning and transformation in PySpark. Databricks processed large datasets efficiently with the single node cluster itself, transforming raw trip data into insights-ready formats.

### 4. Azure Synapse Analytics: 
Acted as the data warehouse for the processed data, enabling efficient querying and storage of structured datasets. The data in Synapse was further utilized to build analytical views for reporting.

### 5. Key Vault for Secure Secret Management:
Stored sensitive information such as ADLS access keys in Azure Key Vault to ensure secure access across the project. This setup reduced hard-coded secrets in code and centralized secret management, enhancing security.
Configured a Key Vault-backed secret scope in Databricks, enabling secure, managed access to ADLS Gen2 directly from the Databricks environment for seamless data processing and transformations.

### 6. Azure Active Directory:
By integrating AAD and enabling role-based access control (RBAC), we maintained a centralized identity management system, enhancing security and simplifying permissions across the project, ensuring that only authorized users and services could access resources like ADLS, Databricks, and Synapse Analytics. 
- For example, granted the Storage Blob Data Contributor role to Databricks for ADLS Gen2 to allow read and write access.

### 7. Power BI:
Created interactive dashboards and heatmap visualizations to present insights on key metrics, such as trip distances, fare amounts, passenger counts, and pickup and drop-off patterns across New York City.


### Data Model:
![Data Model](https://github.com/fero2/NYC-Taxi-Data-Engineering-Project/blob/main/NYC-Taxi%20Data-model.jpg)

- The source dataset contained various data points related to NYC taxi trips, including trip details, location information, fares, and passenger counts. To adhere to a star schema structure, I split the data into several dimension tables (as you can see in the above data model) based on logical grouping and entity definitions.
- The Fact Table was designed to aggregate key metrics and it retained the core structure of the source dataset but was enhanced by including foreign keys that referenced the dimension tables and additional columns.
- Through the relationships established, I was able to create the final analytics table by joining the fact and dimension tables using the primary keys in each dimension table and corresponding foreign keys in the fact table thus allowing and simplifying the process of querying and reporting.


*Note: The entire pipeline can be built in one of the Azure services itself; but different services have been used here just to demonstrate, get an understanding and hands-on experience with popular Azure products*.

### Power BI Dashboard:





## Summary
This end-to-end project shows how Azure’s powerful ecosystem can be leveraged to build and scale a data engineering solution that handles large volumes of real-world data, culminating in impactful analytics and visualizations.
