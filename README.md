# Multinational Retail Data Centralisation (MRDC)

## Table of Contents

1. [Project Description](#project-description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Project Description

MRDC aims to centralise retail data from multiple sources into a single, clean, and easily queryable database. 
The project utilises Python for data extraction, cleaning, and uploading to a PostgreSQL database managed through pgAdmin 4.

### What It Does
- Extracts data from various sources (CSV, APIs, databases, etc.)
- Cleans the data to remove inconsistencies and errors
- Uploads the cleaned data to a PostgreSQL database
- Allows for SQL-based querying of the data for business analytics

### Aim of the Project
- To provide a single source of truth for retail data
- To make the data easily accessible and queryable
- To automate the ETL (Extract, Transform, Load) process

### What I Learned
- Data extraction from various sources
- Data cleaning techniques
- Database management with PostgreSQL and pgAdmin 4
- Python programming for data manipulation and database interactions

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

