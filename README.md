# Urban_Mobility-Data-Explorer

# NYC Taxi Trip Data Explorer

A fullstack web application for analyzing NYC taxi trip data with custom algorithms and interactive visualizations.

## Project Overview

This application processes raw NYC taxi trip records, stores them in a MySQL database, and provides an interactive dashboard to explore urban mobility patterns.

## Technology Stack

- Backend: Python (http.server)
- Database: MySQL
- Frontend: HTML, CSS, JavaScript

## Prerequisites

- Python
- MySQL Server
- Web browser

## Installation Steps

**Clone Repository**

```bash
git clone https://github.com/SHYAKA-Aime/KK_team-Urban_Mobility-Data-Explorer.git
cd KK_team-Urban_Mobility-Data-Explorer
```

### 1. Setup MySQL Database

Open MySQL command line:

```bash
mysql -u root -p
```

Create database and user:

```sql
CREATE DATABASE nyc_taxi_db;
CREATE USER 'taxi_user'@'localhost' IDENTIFIED BY 'taxi_password';
GRANT ALL PRIVILEGES ON nyc_taxi_db.* TO 'taxi_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

### 2. Install Python Package

```bash
pip install mysql-connector-python
```

### 3. Project Structure

```
kk-Team-urban-mobility-explorer/
├── backend/
│   ├── server.py
│   ├── data_processor.py
│   └── algorithms.py
├── frontend/
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── data/
│   └── train.csv
├── schema.sql
└── init_database.py
```

### 4. Configure Database Credentials

change the database configuration in:

- backend/data_processor.py
- backend/server.py
- init_database.py

Change the password to that of mysql DB.

### 5. Place Dataset

Download train.csv and place it in the data folder.

Update the path in backend/data_processor.py

```python
DATA_FILE_PATH = '../data/train.csv'  # or 'data/train.csv'
```

### 6. Initialize Database

```bash
python init_database.py
```

### 7. Process Data

```bash
python backend/data_processor.py
```

This will take 3-5 minutes. You will see progress updates.

### 8. Start Server

```bash
python backend/server.py
```

### 9. Access Application

Open browser to: http://localhost:8000

## Features

### Data Processing

- Validates coordinates within NYC boundaries
- Removes outliers in speed and duration
- Calculates derived features
- Logs all data quality issues

### Derived Features

1. Trip Distance - Haversine formula on GPS coordinates
2. Average Speed - Distance divided by time
3. Trip Efficiency - Distance per minute traveled

### Custom Algorithms

1. QuickSort - Sorts trips by any metric
2. Route Frequency Counter - Finds popular routes
3. IQR Outlier Detection - Identifies anomalies

### API Endpoints

- GET /api/trips - Filtered trip data
- GET /api/statistics - Overall statistics
- GET /api/insights - Data insights
- GET /api/hourly-patterns - Time patterns
- GET /api/top-routes - Popular routes
- GET /api/outliers - Anomaly detection

### Frontend Features

- Real-time statistics display
- Multiple filter options
- Sortable trip table
- Pagination
- Interactive charts

## Database Schema

### trips table

Stores original CSV data with proper data types and indexes.

### trip_metrics table

Stores calculated derived features for each trip.

### data_quality_log table

Records all validation issues and excluded records.

## Usage Examples

### Filtering Trips

Use the filter controls to:

- Select vendor (1 or 2)
- Choose hour of day (0-23)
- Set distance range
- Set duration range
- Filter by weekday/weekend

Click Apply Filters to update the table.

### Viewing Statistics

Statistics cards show:

- Total trips processed
- Average distance
- Average speed
- Average duration

### Analyzing Patterns

Charts display:

- Trips by hour of day
- Trips by day of week
- Distance distribution
- Speed by time period

## API Usage

Get statistics:

```bash
curl http://localhost:8000/api/statistics
```

Get filtered trips:

```bash
curl "http://localhost:8000/api/trips?vendor_id=1&min_distance=5&limit=20"
```

Get top routes:

```bash
curl "http://localhost:8000/api/top-routes?limit=10"
```

## Video Walkthrough

https://youtu.be/ImcdapVb7-o

Contents:

- System architecture overview
- Data processing demonstration
- Custom algorithm explanation
- Dashboard features walkthrough
- Key insights presentation

## Team Members

Aime SHYAKA [SHYAKA-Aime] - Database design, API development
Golbert Gautier Kamanzi [kamanzi2025]- Frontend HTML
Jotham Rutijana Jabo [Rutijana] - UI design and styling
Rwema Christian Gashumba [Rwema707] - Elements Functionalities and Javascript
