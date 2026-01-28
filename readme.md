# Python Data Warehouse

## Technologies Used

- **Docker**
- **PostgreSQL**
- **Python**
   - sqlalchemy
   - pandas
   - python-dotenv
   - plotly
   - dash
   - jupyter

## Docker Setup

### Starting PostgreSQL

```bash
docker compose up -d
```

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