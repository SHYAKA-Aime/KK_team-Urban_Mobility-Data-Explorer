-- Main trips table with all original CSV fields
CREATE TABLE trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    vendor_id TINYINT NOT NULL,
    pickup_datetime DATETIME NOT NULL,
    dropoff_datetime DATETIME NOT NULL,
    passenger_count TINYINT NOT NULL,
    pickup_longitude DECIMAL(11, 8) NOT NULL,
    pickup_latitude DECIMAL(10, 8) NOT NULL,
    dropoff_longitude DECIMAL(11, 8) NOT NULL,
    dropoff_latitude DECIMAL(10, 8) NOT NULL,
    store_and_fwd_flag CHAR(1) DEFAULT 'N',
    trip_duration INT NOT NULL,
    
    -- Indexes for efficient querying
    INDEX idx_pickup_datetime (pickup_datetime),
    INDEX idx_dropoff_datetime (dropoff_datetime),
    INDEX idx_vendor (vendor_id),
    INDEX idx_duration (trip_duration),
    INDEX idx_passenger_count (passenger_count),
    INDEX idx_pickup_location (pickup_longitude, pickup_latitude),
    INDEX idx_dropoff_location (dropoff_longitude, dropoff_latitude)
) ENGINE=InnoDB;

-- Derived metrics table with computed features
CREATE TABLE trip_metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id VARCHAR(50) UNIQUE NOT NULL,
    
    -- Derived Feature 1: Trip Distance (in miles, using Haversine formula)
    trip_distance_miles DECIMAL(10, 4) NOT NULL,
    
    -- Derived Feature 2: Average Speed (mph)
    avg_speed_mph DECIMAL(10, 4) NOT NULL,
    
    -- Derived Feature 3: Trip Efficiency (distance per minute)
    trip_efficiency DECIMAL(10, 6) NOT NULL,
    
    -- Temporal features
    hour_of_day TINYINT NOT NULL,              -- 0-23
    day_of_week TINYINT NOT NULL,              -- 0=Monday, 6=Sunday
    day_of_month TINYINT NOT NULL,             -- 1-31
    month_of_year TINYINT NOT NULL,            -- 1-12
    is_weekend BOOLEAN NOT NULL,
    time_period ENUM('early_morning', 'morning_rush', 'midday', 'evening_rush', 'night', 'late_night') NOT NULL,
    
    -- Trip categorization
    distance_category ENUM('short', 'medium', 'long', 'very_long') NOT NULL,
    duration_category ENUM('quick', 'moderate', 'lengthy', 'extended') NOT NULL,
    speed_category ENUM('slow', 'normal', 'fast') NOT NULL,
    
    -- Indexes for analysis
    INDEX idx_distance (trip_distance_miles),
    INDEX idx_speed (avg_speed_mph),
    INDEX idx_efficiency (trip_efficiency),
    INDEX idx_hour (hour_of_day),
    INDEX idx_day (day_of_week),
    INDEX idx_month (month_of_year),
    INDEX idx_weekend (is_weekend),
    INDEX idx_time_period (time_period),
    INDEX idx_distance_cat (distance_category),
    INDEX idx_duration_cat (duration_category),
    
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Data quality log table to track cleaning decisions
CREATE TABLE data_quality_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    record_id VARCHAR(50),
    issue_type ENUM(
        'missing_values',
        'invalid_coords',
        'invalid_duration',
        'invalid_passenger_count',
        'invalid_datetime',
        'duplicate_record',
        'outlier_distance',
        'outlier_speed',
        'zero_distance',
        'negative_duration'
    ) NOT NULL,
    issue_description VARCHAR(255),
    field_name VARCHAR(50),
    original_value VARCHAR(100),
    action_taken ENUM('excluded', 'corrected', 'flagged') DEFAULT 'excluded',
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_issue_type (issue_type),
    INDEX idx_record_id (record_id)
) ENGINE=InnoDB;

-- Create useful views for common queries

-- View 1: Complete trip details with all computed metrics
CREATE VIEW vw_trip_analysis AS
SELECT 
    t.trip_id,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.pickup_longitude,
    t.pickup_latitude,
    t.dropoff_longitude,
    t.dropoff_latitude,
    t.trip_duration,
    tm.trip_distance_miles,
    tm.avg_speed_mph,
    tm.trip_efficiency,
    tm.hour_of_day,
    tm.day_of_week,
    tm.month_of_year,
    tm.is_weekend,
    tm.time_period,
    tm.distance_category,
    tm.duration_category,
    tm.speed_category,
    CASE tm.day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END AS day_name
FROM trips t
INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id;

-- View 2: Hourly trip statistics
CREATE VIEW vw_hourly_stats AS
SELECT 
    hour_of_day,
    COUNT(*) as trip_count,
    AVG(trip_distance_miles) as avg_distance,
    AVG(avg_speed_mph) as avg_speed,
    AVG(trip_duration) as avg_duration,
    SUM(trip_distance_miles) as total_distance
FROM vw_trip_analysis
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- View 3: Daily statistics
CREATE VIEW vw_daily_stats AS
SELECT 
    day_of_week,
    day_name,
    is_weekend,
    COUNT(*) as trip_count,
    AVG(trip_distance_miles) as avg_distance,
    AVG(avg_speed_mph) as avg_speed,
    AVG(trip_duration) as avg_duration
FROM vw_trip_analysis
GROUP BY day_of_week, day_name, is_weekend
ORDER BY day_of_week;

-- View 4: Vendor comparison
CREATE VIEW vw_vendor_stats AS
SELECT 
    vendor_id,
    COUNT(*) as trip_count,
    AVG(trip_distance_miles) as avg_distance,
    AVG(avg_speed_mph) as avg_speed,
    AVG(trip_duration) as avg_duration,
    AVG(passenger_count) as avg_passengers
FROM trips t
INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id
GROUP BY vendor_id;