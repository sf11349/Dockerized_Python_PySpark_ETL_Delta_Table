# Building a Dockerized PySpark ETL Pipeline with Delta Table. 


The pipeline extracts data from data.csv, transform it and load to Sparks Delta Table. See the Brief.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [Docker Build](#docker)
- [Contact Information](#contact-information)

## Installation

1. Ensure you have Python 3.9 or higher installed.
2. Clone this repository: `git clone https://github.com/sf11349/Dockerized_Python_PySpark_ETL_Delta_Table.git`
3. Navigate to the project directory: `cd repo`
4. Install the dependencies: `pip install -r requirements.txt`
5. Install Docker 

## Usage

1. Run the project: `python etl.py`
2. For Docker see example below on how to run it



## File Structure

The project follows the following structure:

```bash
Code language: Python (python)
project/
├── etl.py
├── etl_stages/
│ ├── extract.py
│ ├── transform.py
│ └── load.py
├── data/
│ ├── source / data.csv
│ ├── sink / CustomerOrders (Delta Table)
├── utils/
│ ├── utils.py
└── README.md
└── requirements.txt
└── Dockerfile
└── .ignore files...




## Docker

To run Docker:

```python
docker build -t etl_image .
docker run --name shawan_app etl_image
```

## Contact Information
shawan.faily@azuresystems.co.uk


