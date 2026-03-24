# Python Data Warehouse

Personal project where I create an ETL with the medallion architecture for training in Python. Also, I've made a mini-dashboard with dash and plotly.

## Technologies Used

- **Docker**
- **PostgreSQL**
- **Python**
   - sqlalchemy
   - pandas
   - python-dotenv
   - plotly
   - dash

## Docker Setup

### Starting PostgreSQL

```bash
docker compose up -d
```

You do defined a docker here, maybe add into the readme a section to explain to create a python env for the requirements.

## Python Scripts

**Ensure PostgreSQL service is running before executing scripts**

### Installation

```bash
# View all dependencies
pip freeze

# Install dependencies
pip install -r requirements.txt
```

### Running ETL

```bash
python3 ./etl.py
```

### BI Dashboard

```bash
python3 ./dashboardBI
```

## Dataset

Dataset source: [Kaggle - Café Sales Dirty Data](https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training)
