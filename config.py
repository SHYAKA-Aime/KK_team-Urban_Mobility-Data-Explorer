# Configuration file for NYC Taxi Data Explorer

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'bruest2500',
    'database': 'kk_team_nyc_taxi_db'
}

# Server configurationa
SERVER_HOST = 'localhost'
SERVER_PORT = 8000

# Data processing parameters
DATA_FILE_PATH = 'data/train.csv'
BATCH_SIZE = 1000  # For batch inserts

# Data validation thresholds
MIN_TRIP_DURATION = 60  # seconds
MAX_TRIP_DURATION = 86400  # 24 hours in seconds
MIN_PASSENGER_COUNT = 1
MAX_PASSENGER_COUNT = 9
NYC_LAT_MIN = 40.4774
NYC_LAT_MAX = 40.9176
NYC_LON_MIN = -74.2591
NYC_LON_MAX = -73.7004

# Speed thresholds (mph)
MIN_SPEED = 0.5
MAX_SPEED = 100

# Distance thresholds (miles)
MAX_TRIP_DISTANCE = 100