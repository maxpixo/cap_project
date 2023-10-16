# Capstone Project - ETL and Data Analysis

This repository contains the code and documentation for the Capstone Project, which focuses on Extract, Transform, Load (ETL) processes and data analysis using various technologies.
## Table of Contents

1. [Project Overview](#project-overview)
2. [Technologies Used](#technologies-used)
3. [ETL Process](#etl-process)
   - [Credit Card Dataset ETL](#credit-card-dataset-etl)
   - [Loan Application Dataset ETL](#loan-application-dataset-etl)
4. [Application Front-End](#application-front-end)
5. [Data Analysis and Visualization](#data-analysis-and-visualization)
   - [Credit Card Dataset Analysis](#credit-card-dataset-analysis)
   - [Loan Application Dataset Analysis](#loan-application-dataset-analysis)
6. [Installation](#installation)
7. [Usage](#usage)
8. [Contributing](#contributing)
9. [License](#license)

## Project Overview

The Capstone Project involves working with two datasets, Credit Card data, and Loan Application data. It focuses on the following key aspects:

1. ETL process for loading the Credit Card dataset into an RDBMS using Python, PySpark, and SQL.
2. Development of a console-based Python program to display and manage data from the loaded dataset.
3. Data analysis and visualization using Python libraries.

## Technologies Used

The project uses the following technologies:

- Python
- PySpark
- SQL (MySQL)
- Apache Spark (Spark Core, Spark SQL)
- Python Visualization and Analytics librarie (Matplotlib)

## ETL Process

### Credit Card Dataset ETL

The ETL process for the Credit Card dataset involves the following steps:

1. Data Extraction and Transformation: Python and PySpark are used to read and extract data from the following JSON files:
   - CDW_SAPP_BRANCH.JSON
   - CDW_SAPP_CREDITCARD.JSON
   - CDW_SAPP_CUSTOMER.JSON
   Data transformation is performed as per the mapping document.

2. Data Loading into Database:
   - A database named "creditcard_capstone" is created in MySQL.
   - Python and PySpark scripts are used to load data into the database, creating tables with the following names:
     - CDW_SAPP_BRANCH
     - CDW_SAPP_CREDIT_CARD
     - CDW_SAPP_CUSTOMER

### Loan Application Dataset ETL

The ETL process for the Loan Application dataset involves:

1. Consuming data from a REST API endpoint for loan applications.

2. Finding the status code of the API endpoint.

3. Loading data into the "creditcard_capstone" database using PySpark with the table name "CDW-SAPP_loan_application."

## Application Front-End

A console-based Python program is created to interact with the loaded data. It includes two main modules:

1. Transaction Details Module: Displays transaction information based on user queries.
2. Customer Details Module: Provides access to customer account details and transaction history.

## Data Analysis and Visualization

Data analysis and visualization are performed on both datasets using Python libraries. The following analyses are conducted:

### Credit Card Dataset Analysis

1. Finding and plotting the transaction type with the highest transaction count.
2. Finding and plotting the state with the highest number of customers.
3. Finding and plotting the sum of all transactions for the top 10 customers and identifying the customer with the highest transaction amount.

### Loan Application Dataset Analysis

1. Finding and plotting the percentage of applications approved for self-employed applicants.
2. Finding the percentage of rejection for married male applicants.
3. Finding and plotting the top three months with the largest volume of transaction data.
4. Finding and plotting which branch processed the highest total dollar value of healthcare transactions.

## Installation

To run this project on your local machine, you'll need to set up the required environment and dependencies. You can use the `requirements.txt` file to install the necessary Python packages.

1. Create a virtual environment (optional but recommended):

   bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use 'venv\Scripts\activate'
   

2. Install the required packages:

   bash
   pip install -r requirements.txt
   

3. Set up a MySQL database with the name "creditcard_capstone" and configure the database connection in the code.

4. Clone the project repository from your private GitHub repository.

5. Ensure that sensitive information (e.g., database credentials) is properly configured and .gitignore is set up.

## Usage

To run the project, execute the main Python program (app.py).

bash
python app.py


## Contributing

Contributions to this project are welcome. If you find any issues or have suggestions for improvements, please open an issue or create a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.