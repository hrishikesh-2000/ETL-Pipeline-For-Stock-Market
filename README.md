# 📈 ETL Pipeline for Stock Market 📉

An end-to-end ETL (Extract, Transform, Load) pipeline designed to process and analyze stock market data using **PySpark** for distributed data processing. 
This project enables data ingestion, transformation, and query-ready storage, structured in **Staging**, **Silver**, and **Gold** layers using PySpark API

## 📑 Table of Contents

- [📋 Project Overview](#project-overview)
- [🏗️ Architecture](#architecture)
- [✨ Features](#features)
- [💻 Technologies Used](#technologies-used)
- [📊 Data Sources](#data-sources)
- [🚀 Usage](#usage)
- [🌟 Future Enhancements](#future-enhancements)
- [🤝 Contributing](#contributing)

---

## 📋 Project Overview

This project gives a demo of the data ingestion, processing, and transformation of stock market data. 
It enables data collection from various sources, cleansing, transformation, and storage in a format ready for analytical querying. 

### Project Highlights:
- **Staging Layer**: 🗃️ Ingests and stores raw data.
- **Silver Layer**: 🛠️ Cleanses and joins data to get some historical insights data.
- **Gold Layer**: 🏆 Aggregates and enriches data for analytical use.

## 🏗️ Architecture

The architecture follows a **Medallion** approach with three layers.

<div align="center">
<img src="https://github.com/hrishikesh-2000/ETL-Pipeline-For-Stock-Market/blob/Initial_dev/schema/Stock%20Market%20Schema.png" alt="ETL Pipeline Diagram" width="500"/>
</div>

## ✨ Features

- **🔌 Data Ingestion**: Extraction of stock data from CSVs and APIs.
- **🧼 Data Transformation**: Data cleaning and transformation using PySpark.
- **🔍 SQL Compatibility**: Creates global temporary views for SQL querying.
- **📅 Weekly & Monthly Summaries**: Aggregates and calculates metrics like SMA and returns.

## 💻 Technologies Used

- **Python** 🐍
- **PySpark** ⚡ - for distributed data processing
- **Pandas** 🐼 - for data handling
- **Upstox API** 🔗 - for stock market data
- **CSV** 📄 - for metadata and static data files

## 📊 Data Sources

- **Upstox API**: Fetches real-time and historical stock data 📈.
- **NSE CSV Files**: Provides metadata for stock information 📃.

## 🚀 Usage
- Run Staging.py Script: 🗃️ Loads raw data into staging tables.
- Run Silver.py Script: 🔄 Cleans and integrates the data.
- Run main.py Script: 📊 Creates weekly and monthly summaries for analysis.

## 🌟 Future Enhancements
- 🔄 Automated Scheduler: Integrate with Apache Airflow for scheduled runs.
- 🔍 Data Validation: Implement data checks at each stage.
- ⚡ Real-Time Data Integration: Add streaming data capabilities.
- 📊 Interactive Dashboard: Visualize data using Tableau or Power BI.
- ⚠️ Error Handling & Logging

## 🤝 Contributing
Contributions and feedback are welcome! 🎉 Follow the steps below to contribute:

- Fork the repository 🍴.
- Create a new branch (git checkout -b feature-branch).
- Make your changes and commit 📝.
- Push to the branch (git push origin feature-branch).
- Open a pull request 🚀.
---

### Prerequisites

- Python 2.9 🐍
- Apache Spark and PySpark ⚡
- Upstox API account 🔑

### Setup Instructions

1. **Clone the Repository**
2. Install Dependencies: Use requirements.txt for easy package installation
3. Spark Configuration: Ensure the spark_config.py script initiates a Spark session
4. Prepare Data Files: Place necessary CSV files in the data/ directory

