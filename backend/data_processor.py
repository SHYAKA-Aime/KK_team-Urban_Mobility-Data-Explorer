import csv
import mysql.connector
from datetime import datetime
import math

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'bruest2500',
    'database': 'kk_team_nyc_taxi_db'
}


# Data processing parameters
DATA_FILE_PATH = 'data/train.csv'  # Adjust path if needed
BATCH_SIZE = 1000

# Data validation thresholds
MIN_TRIP_DURATION = 60
MAX_TRIP_DURATION = 86400
MAX_RECORDS = 300
MIN_PASSENGER_COUNT = 1
MAX_PASSENGER_COUNT = 9
NYC_LAT_MIN = 40.4774
NYC_LAT_MAX = 40.9176
NYC_LON_MIN = -74.2591
NYC_LON_MAX = -73.7004
MIN_SPEED = 0.5
MAX_SPEED = 100
MAX_TRIP_DISTANCE = 100

class DataProcessor:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.issues_log = []
        self.stats = {
            'total': 0,
            'valid': 0,
            'invalid': 0,
            'duplicates': 0
        }
        
    def connect_db(self):
        """Connect to MySQL database"""
        try:
            self.conn = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print("Connected to database successfully")
        except mysql.connector.Error as err:
            print(f"Database connection failed: {err}")
            raise
        
    def disconnect_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")
    
    def haversine_distance(self, lon1, lat1, lon2, lat2):
      
        R = 3959  # Earth's radius in miles
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        # Haversine formula
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def validate_record(self, row):
      
        issues = []
        
        # Check for missing values
        required_fields = ['id', 'vendor_id', 'pickup_datetime', 'dropoff_datetime', 
                          'passenger_count', 'pickup_longitude', 'pickup_latitude',
                          'dropoff_longitude', 'dropoff_latitude', 'trip_duration']
        
        for field in required_fields:
            if not row.get(field) or row[field].strip() == '':
                issues.append(('missing_values', f'Missing {field}', field, ''))
                return False, issues
        
        try:
            # Validate coordinates
            pickup_lon = float(row['pickup_longitude'])
            pickup_lat = float(row['pickup_latitude'])
            dropoff_lon = float(row['dropoff_longitude'])
            dropoff_lat = float(row['dropoff_latitude'])
            
            # Check if coordinates are in NYC bounds
            if not (NYC_LON_MIN <= pickup_lon <= NYC_LON_MAX and NYC_LAT_MIN <= pickup_lat <= NYC_LAT_MAX):
                issues.append(('invalid_coords', 'Pickup outside NYC', 'pickup_coords', 
                             f'{pickup_lon},{pickup_lat}'))
                return False, issues
                
            if not (NYC_LON_MIN <= dropoff_lon <= NYC_LON_MAX and NYC_LAT_MIN <= dropoff_lat <= NYC_LAT_MAX):
                issues.append(('invalid_coords', 'Dropoff outside NYC', 'dropoff_coords',
                             f'{dropoff_lon},{dropoff_lat}'))
                return False, issues
            
            # Check for zero distance trips
            if abs(pickup_lon - dropoff_lon) < 0.0001 and abs(pickup_lat - dropoff_lat) < 0.0001:
                issues.append(('zero_distance', 'Pickup and dropoff same location', 'coordinates', ''))
                return False, issues
            
            # Validate trip duration
            duration = int(row['trip_duration'])
            if duration <= 0:
                issues.append(('negative_duration', 'Duration is negative or zero', 'trip_duration', str(duration)))
                return False, issues
            
            if duration < MIN_TRIP_DURATION:
                issues.append(('invalid_duration', f'Duration too short: {duration}s', 'trip_duration', str(duration)))
                return False, issues
                
            if duration > MAX_TRIP_DURATION:
                issues.append(('invalid_duration', f'Duration too long: {duration}s', 'trip_duration', str(duration)))
                return False, issues
            
            # Validate passenger count
            passenger_count = int(row['passenger_count'])
            if passenger_count < MIN_PASSENGER_COUNT or passenger_count > MAX_PASSENGER_COUNT:
                issues.append(('invalid_passenger_count', f'Invalid count: {passenger_count}', 
                             'passenger_count', str(passenger_count)))
                return False, issues
            
            # Validate datetime
            pickup_dt = datetime.strptime(row['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
            dropoff_dt = datetime.strptime(row['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
            
            if dropoff_dt <= pickup_dt:
                issues.append(('invalid_datetime', 'Dropoff before or equal to pickup', 'datetime', ''))
                return False, issues
            
            # Calculate actual duration and compare with recorded duration
            actual_duration = (dropoff_dt - pickup_dt).total_seconds()
            if abs(actual_duration - duration) > 10:  # Allow 10 second tolerance
                issues.append(('invalid_duration', 'Duration mismatch with timestamps', 
                             'trip_duration', f'recorded:{duration}, actual:{actual_duration}'))
                return False, issues
                
        except (ValueError, TypeError) as e:
            issues.append(('invalid_datetime', f'Parse error: {str(e)}', 'datetime', ''))
            return False, issues
        
        return True, issues
    
    def categorize_distance(self, distance):
        """Categorize trip distance"""
        if distance < 1:
            return 'short'
        elif distance < 5:
            return 'medium'
        elif distance < 15:
            return 'long'
        else:
            return 'very_long'
    
    def categorize_duration(self, duration):
        """Categorize trip duration (in seconds)"""
        minutes = duration / 60
        if minutes < 10:
            return 'quick'
        elif minutes < 30:
            return 'moderate'
        elif minutes < 60:
            return 'lengthy'
        else:
            return 'extended'
    
    def categorize_speed(self, speed):
        """Categorize average speed"""
        if speed < 10:
            return 'slow'
        elif speed < 25:
            return 'normal'
        else:
            return 'fast'
    
    def get_time_period(self, hour):
        """Determine time period based on hour"""
        if 0 <= hour < 5:
            return 'late_night'
        elif 5 <= hour < 7:
            return 'early_morning'
        elif 7 <= hour < 10:
            return 'morning_rush'
        elif 10 <= hour < 16:
            return 'midday'
        elif 16 <= hour < 20:
            return 'evening_rush'
        else:
            return 'night'
    
    def compute_derived_features(self, row, trip_distance):
   
        pickup_dt = datetime.strptime(row['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        duration_seconds = int(row['trip_duration'])
        duration_hours = duration_seconds / 3600
        duration_minutes = duration_seconds / 60
        
        # DERIVED FEATURE 1: Trip Distance (already calculated)
        # trip_distance_miles = trip_distance
        
        # DERIVED FEATURE 2: Average Speed (mph)
        avg_speed_mph = trip_distance / duration_hours if duration_hours > 0 else 0
        
        # DERIVED FEATURE 3: Trip Efficiency (distance per minute)
        # This measures how much distance is covered per minute
        # Higher values indicate more efficient/direct routes
        trip_efficiency = trip_distance / duration_minutes if duration_minutes > 0 else 0
        
        # Temporal features
        hour_of_day = pickup_dt.hour
        day_of_week = pickup_dt.weekday()  # 0=Monday, 6=Sunday
        day_of_month = pickup_dt.day
        month_of_year = pickup_dt.month
        is_weekend = day_of_week >= 5
        time_period = self.get_time_period(hour_of_day)
        
        # Categorical features
        distance_category = self.categorize_distance(trip_distance)
        duration_category = self.categorize_duration(duration_seconds)
        speed_category = self.categorize_speed(avg_speed_mph)
        
        return {
            'trip_distance_miles': round(trip_distance, 4),
            'avg_speed_mph': round(avg_speed_mph, 4),
            'trip_efficiency': round(trip_efficiency, 6),
            'hour_of_day': hour_of_day,
            'day_of_week': day_of_week,
            'day_of_month': day_of_month,
            'month_of_year': month_of_year,
            'is_weekend': is_weekend,
            'time_period': time_period,
            'distance_category': distance_category,
            'duration_category': duration_category,
            'speed_category': speed_category
        }
    
    def log_issue(self, record_id, issue_type, description, field_name, value):
        """Log data quality issue"""
        self.issues_log.append({
            'record_id': record_id,
            'issue_type': issue_type,
            'description': description,
            'field_name': field_name,
            'value': str(value)[:100]  # Limit value length
        })
    
    def process_and_load_data(self):
        """Main function to process CSV and load into database"""
        print("\n")
        print("DATA PROCESSING PIPELINE")
        print("\n")
        
        print("Reading data from:", DATA_FILE_PATH)
        
        valid_records = []
        seen_ids = set()
        
        try:
            with open(DATA_FILE_PATH, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                MAX_VALID_RECORDS = 300

                for row in reader:
                    if self.stats['valid'] >= MAX_VALID_RECORDS:
                        print(f"Reached limit of {MAX_VALID_RECORDS} valid records.")
                        break

                    self.stats['total'] += 1

                    
                    # Check for duplicates
                    if row['id'] in seen_ids:
                        self.stats['duplicates'] += 1
                        self.log_issue(row['id'], 'duplicate_record', 'Duplicate trip ID', 'id', row['id'])
                        continue
                    
                    seen_ids.add(row['id'])
                    
                    # Validate record
                    is_valid, issues = self.validate_record(row)
                    
                    if not is_valid:
                        self.stats['invalid'] += 1
                        for issue_type, description, field_name, value in issues:
                            self.log_issue(row['id'], issue_type, description, field_name, value)
                        continue
                    
                    # Calculate distance
                    trip_distance = self.haversine_distance(
                        float(row['pickup_longitude']),
                        float(row['pickup_latitude']),
                        float(row['dropoff_longitude']),
                        float(row['dropoff_latitude'])
                    )
                    
                    # Check if distance is reasonable
                    if trip_distance > MAX_TRIP_DISTANCE:
                        self.stats['invalid'] += 1
                        self.log_issue(row['id'], 'outlier_distance', 
                                     f'Distance {trip_distance:.2f} miles exceeds maximum',
                                     'distance', f'{trip_distance:.2f}')
                        continue
                    
                    # Compute speed and check validity
                    duration_hours = int(row['trip_duration']) / 3600
                    trip_speed = trip_distance / duration_hours if duration_hours > 0 else 0
                    
                    if trip_speed < MIN_SPEED or trip_speed > MAX_SPEED:
                        self.stats['invalid'] += 1
                        self.log_issue(row['id'], 'outlier_speed',
                                     f'Speed {trip_speed:.2f} mph is unrealistic',
                                     'speed', f'{trip_speed:.2f}')
                        continue
                    
                    # Compute derived features
                    features = self.compute_derived_features(row, trip_distance)
                    
                    # Prepare valid record
                    valid_records.append({
                        'row': row,
                        'features': features
                    })
                    
                    self.stats['valid'] += 1
                    
                    # Batch insert
                    if len(valid_records) >= BATCH_SIZE:
                        self.insert_batch(valid_records)
                        valid_records = []
                        print(f"Processed {self.stats['total']} records " +
                              f"(Valid: {self.stats['valid']}, Invalid: {self.stats['invalid']})")
            
            # Insert remaining records
            if valid_records:
                self.insert_batch(valid_records)
            
            # Insert issues log
            self.insert_issues_log()
            
            # Print summary
            self.print_summary()
            
        except FileNotFoundError:
            print(f"Error: File not found - {DATA_FILE_PATH}")
            print("  Please ensure train.csv is in the data/ directory")
            raise
        except Exception as e:
            print(f"Error during processing: {e}")
            raise
    
    def insert_batch(self, records):
        """Insert batch of records into database"""
        try:
            # Insert trips
            trip_data = []
            for rec in records:
                row = rec['row']
                trip_data.append((
                    row['id'],
                    int(row['vendor_id']),
                    row['pickup_datetime'],
                    row['dropoff_datetime'],
                    int(row['passenger_count']),
                    float(row['pickup_longitude']),
                    float(row['pickup_latitude']),
                    float(row['dropoff_longitude']),
                    float(row['dropoff_latitude']),
                    row['store_and_fwd_flag'],
                    int(row['trip_duration'])
                ))
            
            self.cursor.executemany(
                """INSERT INTO trips (trip_id, vendor_id, pickup_datetime, dropoff_datetime,
                   passenger_count, pickup_longitude, pickup_latitude, dropoff_longitude, 
                   dropoff_latitude, store_and_fwd_flag, trip_duration)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                trip_data
            )
            
            # Insert metrics
            metrics_data = []
            for rec in records:
                row = rec['row']
                features = rec['features']
                metrics_data.append((
                    row['id'],
                    features['trip_distance_miles'],
                    features['avg_speed_mph'],
                    features['trip_efficiency'],
                    features['hour_of_day'],
                    features['day_of_week'],
                    features['day_of_month'],
                    features['month_of_year'],
                    features['is_weekend'],
                    features['time_period'],
                    features['distance_category'],
                    features['duration_category'],
                    features['speed_category']
                ))
            
            self.cursor.executemany(
                """INSERT INTO trip_metrics (trip_id, trip_distance_miles, avg_speed_mph, 
                   trip_efficiency, hour_of_day, day_of_week, day_of_month, month_of_year,
                   is_weekend, time_period, distance_category, duration_category, speed_category)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                metrics_data
            )
            
            self.conn.commit()
            
        except mysql.connector.Error as err:
            print(f"Error inserting batch: {err}")
            self.conn.rollback()
            raise
    
    def insert_issues_log(self):
        """Insert data quality issues into log table"""
        if not self.issues_log:
            return
        
        try:
            log_data = [(
                issue['record_id'],
                issue['issue_type'],
                issue['description'],
                issue['field_name'],
                issue['value']
            ) for issue in self.issues_log]
            
            self.cursor.executemany(
                """INSERT INTO data_quality_log (record_id, issue_type, issue_description, 
                   field_name, original_value)
                   VALUES (%s, %s, %s, %s, %s)""",
                log_data
            )
            
            self.conn.commit()
            print(f"Logged {len(self.issues_log)} data quality issues")
            
        except mysql.connector.Error as err:
            print(f"Error logging issues: {err}")
    
    def print_summary(self):
        """Print processing summary"""
        print("\n")
        print("PROCESSING SUMMARY")
        print(f"Total records processed:    {self.stats['total']:,}")
        print(f"Valid records inserted:     {self.stats['valid']:,}")
        print(f"Invalid records excluded:   {self.stats['invalid']:,}")
        print(f"Duplicate records skipped:  {self.stats['duplicates']:,}")
        
        if self.stats['total'] > 0:
            success_rate = (self.stats['valid']/self.stats['total']*100)
            print(f"Success rate:               {success_rate:.2f}%")
        
        print("\n")
        
        # Issue breakdown
        if self.issues_log:
            issue_counts = {}
            for issue in self.issues_log:
                issue_type = issue['issue_type']
                issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
            
            print("ISSUE BREAKDOWN:")
            for issue_type, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"  {issue_type:<25} {count:>6,}")
            print()


def main():
    """Main execution function"""
    processor = DataProcessor()
    
    try:
        # Connect to database
        processor.connect_db()
        
        # Process and load data
        processor.process_and_load_data()
        
        print("Data processing completed successfully!")
        print("\nYou can now start the backend server with:")
        print("  python backend/server.py")
        
    except Exception as e:
        print(f"\nFatal error: {e}")
        return 1
    finally:
        processor.disconnect_db()
    
    return 0


if __name__ == "__main__":
    exit(main())