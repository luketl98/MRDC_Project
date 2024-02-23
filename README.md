# Multinational Retail Data Centralisation (MRDC)

## Table of Contents

1. [Project Description](#project-description)
2. [Features](#features)
3. [Technologies Used](#technologies-used)
4. [Installation](#installation)
5. [Usage](#usage)
6. [File Structure](#file-structure)
7. [License](#license)

## Project Description

MRDC aims to centralise retail data from multiple sources into a single, clean, and easily queryable database. 
The project is part of an AICore course and is designed to serve as a single source of truth for sales data for a multinational retail company. 
It extracts, cleans, and uploads data from various sources, making it easily accessible and analyzable for team members.

### Motivation

The motivation behind this project is to solve the problem of scattered sales data across different data sources in a multinational retail company. 
The project aims to streamline data accessibility and analysis by centralising this data.

### Problem Statement

- Historical data of users is stored in an AWS database
- Users' card details are stored in PDF documents in an AWS S3 bucket
- Store data can be retrieved through an API
- Product information is stored in CSV format in an AWS S3 bucket
- Sale details are stored in a JSON file

## Features

- Data extraction from multiple sources including AWS databases, S3 buckets, and APIs
- Data cleaning to prepare for database storage
- Database schema creation and management
- Querying capabilities for up-to-date business metrics

## Technologies Used

- Python for data extraction, cleaning, and database management
- PostgreSQL as the database
- AWS for cloud storage
- pgAdmin 4 for database administration

## Installation

1. Clone the repository: `git clone https://github.com/YourUsername/MRDC.git`
2. Install required Python packages: `pip install -r requirements.txt`
3. Set up a PostgreSQL database through pgAdmin 4.

## Usage

1. Update `local_creds.yaml` with your local system credentials.
2. Update `db_creds.yaml` with your database credentials.
3. Run `main.py` to start the ETL process: 'python main.py'
4. Open pgAdmin 4 to query the database.

## File Structure

- `main.py`: The entry point for the ETL process.
- `data_extraction.py`: Responsible for extracting data from various sources.
- `data_cleaning.py`: Cleans the extracted data.
- `database_utils.py`: Contains utility functions for database operations.
- `cast_data_types.sql`: SQL file for casting data types in the database.

## License

MIT License. See `LICENSE.md` for more details.

