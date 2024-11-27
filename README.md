# Job Recommendation System Implementation

This project implements a job recommendation system using a job description dataset sourced from Kaggle. The system follows the Medallion Architecture, processing data through the Bronze, Silver, and Gold layers, all powered by Apache Spark.

## Project Structure

- **Initial_preprocessing_using_pandas.py**: Initial data preprocessing using Pandas.
- **Preprocessing_Data.ipynb**: Jupyter Notebook for data cleaning and transformation.
- **Processing_Data_Only_Spark.py**: Data processing using Spark-only operations.
- **initial_batching_using_pandas.ipynb**: Batching implementation with Pandas.
- **limiting_records_helper_function.py**: Helper functions for record management.
- **removing_columns.ipynb**: Notebook for column removal and data refinement.
- **skilled_demand_analysis.ipynb**: Analysis of skill demand from job descriptions.
- **skilled_job_analysis_edited.ipynb**: Edited analysis of job skills.
- **structured_to_unstructured.ipynb**: Conversion of structured data to unstructured format.

## Overview

The project processes job description data through the Medallion Architecture:

- **Bronze Layer**: Raw data ingestion and initial storage.
- **Silver Layer**: Data cleaning and transformation to ensure quality and consistency.
- **Gold Layer**: Aggregated and refined data, optimized for analytics and insights.

## Technology Stack

- **Apache Spark**: Used for distributed data processing and analysis.
- **Pandas**: Initially used for data manipulation before transitioning to Spark.

## Development Process

The project began with data processing using familiar libraries like Pandas. It was then reworked to utilize Spark-only operations, enhancing scalability and efficiency for large datasets.

## Getting Started

1. Clone the repository.
2. Set up your Spark environment.
3. Follow the notebooks and scripts to process and analyze the job description data.

## Acknowledgments

- Dataset sourced from [Kaggle](https://www.kaggle.com/).
