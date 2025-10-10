import http.server
import socketserver
import json
import urllib.parse
from pathlib import Path
from decimal import Decimal
import mysql.connector
from algorithms import QuickSort, RouteFrequencyCounter, OutlierDetector, TimeSeriesGrouper

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'bruest2500',
    'database': 'kk_team_nyc_taxi_db'
}

# Server configuration
SERVER_HOST = 'localhost'
SERVER_PORT = 8000


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class TaxiAPIHandler(http.server.BaseHTTPRequestHandler):
    """Custom HTTP request handler for NYC Taxi API"""
    
    def _set_cors_headers(self):
        """Set CORS headers to allow cross-origin requests"""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
    
    def _set_json_headers(self):
        """Set headers for JSON response"""
        self.send_header('Content-type', 'application/json')
        self._set_cors_headers()
        self.end_headers()
    
    def _set_html_headers(self):
        """Set headers for HTML response"""
        self.send_header('Content-type', 'text/html')
        self._set_cors_headers()
        self.end_headers()
    
    def _set_css_headers(self):
        """Set headers for CSS response"""
        self.send_header('Content-type', 'text/css')
        self._set_cors_headers()
        self.end_headers()
    
    def _set_js_headers(self):
        """Set headers for JavaScript response"""
        self.send_header('Content-type', 'application/javascript')
        self._set_cors_headers()
        self.end_headers()
    
    def _convert_decimals(self, data):
        """Recursively convert Decimal objects to float in nested structures"""
        if isinstance(data, list):
            return [self._convert_decimals(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._convert_decimals(value) for key, value in data.items()}
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response with proper encoding"""
        self.send_response(status_code)
        self._set_json_headers()
        # Convert all Decimals before encoding
        converted_data = self._convert_decimals(data)
        self.wfile.write(json.dumps(converted_data).encode('utf-8'))
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS"""
        self.send_response(200)
        self._set_cors_headers()
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests"""
        try:
            # Parse URL and query parameters
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path
            query_params = urllib.parse.parse_qs(parsed_path.query)
            
            # Route requests
            if path == '/' or path == '/index.html':
                self.serve_file('frontend/index.html', 'html')
            elif path == '/styles.css':
                self.serve_file('frontend/styles.css', 'css')
            elif path == '/app.js':
                self.serve_file('frontend/app.js', 'js')
            elif path == '/api/trips':
                self.handle_get_trips(query_params)
            elif path == '/api/statistics':
                self.handle_get_statistics()
            elif path == '/api/insights':
                self.handle_get_insights()
            elif path == '/api/hourly-patterns':
                self.handle_hourly_patterns()
            elif path == '/api/top-routes':
                self.handle_top_routes(query_params)
            elif path == '/api/outliers':
                self.handle_outliers(query_params)
            else:
                self.send_error(404, "Endpoint not found")
        except Exception as e:
            print(f"Error in do_GET: {str(e)}")
            self.send_error(500, f"Server error: {str(e)}")
    
    def serve_file(self, filepath, file_type):
        """Serve static files"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            self.send_response(200)
            if file_type == 'html':
                self._set_html_headers()
            elif file_type == 'css':
                self._set_css_headers()
            elif file_type == 'js':
                self._set_js_headers()
            
            self.wfile.write(content.encode('utf-8'))
        except FileNotFoundError:
            self.send_error(404, f"File not found: {filepath}")
        except Exception as e:
            self.send_error(500, f"Server error: {str(e)}")
    
    def get_db_connection(self):
        """Create database connection"""
        return mysql.connector.connect(**DB_CONFIG)
    
    def handle_get_trips(self, params):
        """
        GET /api/trips
        Query parameters:
            - limit: number of records (default 100)
            - offset: pagination offset (default 0)
            - sort_by: field to sort by (distance, duration, speed)
            - order: asc or desc
            - min_distance, max_distance: filter by distance
            - min_duration, max_duration: filter by duration
            - vendor_id: filter by vendor
            - hour: filter by hour of day
            - day_of_week: filter by day (0-6)
        """
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Parse parameters
            limit = int(params.get('limit', [100])[0])
            offset = int(params.get('offset', [0])[0])
            sort_by = params.get('sort_by', ['pickup_datetime'])[0]
            order = params.get('order', ['desc'])[0]
            
            # Build WHERE clause
            where_conditions = []
            query_params = []
            
            if 'min_distance' in params:
                where_conditions.append("tm.trip_distance_miles >= %s")
                query_params.append(float(params['min_distance'][0]))
            
            if 'max_distance' in params:
                where_conditions.append("tm.trip_distance_miles <= %s")
                query_params.append(float(params['max_distance'][0]))
            
            if 'min_duration' in params:
                where_conditions.append("t.trip_duration >= %s")
                query_params.append(int(params['min_duration'][0]))
            
            if 'max_duration' in params:
                where_conditions.append("t.trip_duration <= %s")
                query_params.append(int(params['max_duration'][0]))
            
            if 'vendor_id' in params:
                where_conditions.append("t.vendor_id = %s")
                query_params.append(int(params['vendor_id'][0]))
            
            if 'hour' in params:
                where_conditions.append("tm.hour_of_day = %s")
                query_params.append(int(params['hour'][0]))
            
            if 'day_of_week' in params:
                where_conditions.append("tm.day_of_week = %s")
                query_params.append(int(params['day_of_week'][0]))
            
            if 'is_weekend' in params:
                where_conditions.append("tm.is_weekend = %s")
                query_params.append(params['is_weekend'][0].lower() == 'true')
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Map sort field
            sort_field_map = {
                'distance': 'tm.trip_distance_miles',
                'duration': 't.trip_duration',
                'speed': 'tm.avg_speed_mph',
                'pickup_datetime': 't.pickup_datetime'
            }
            sort_field = sort_field_map.get(sort_by, 't.pickup_datetime')
            
            # Build query
            query = f"""
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
                    tm.is_weekend,
                    tm.time_period,
                    tm.distance_category,
                    tm.duration_category
                FROM trips t
                INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id
                {where_clause}
                ORDER BY {sort_field} {order.upper()}
                LIMIT %s OFFSET %s
            """
            
            query_params.extend([limit, offset])
            cursor.execute(query, query_params)
            
            trips = cursor.fetchall()
            
            # Convert datetime to string
            for trip in trips:
                if trip.get('pickup_datetime'):
                    trip['pickup_datetime'] = str(trip['pickup_datetime'])
                if trip.get('dropoff_datetime'):
                    trip['dropoff_datetime'] = str(trip['dropoff_datetime'])
            
            # Get total count
            count_query = f"""
                SELECT COUNT(*) as total
                FROM trips t
                INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id
                {where_clause}
            """
            cursor.execute(count_query, query_params[:-2])  # Exclude limit and offset
            total_count = cursor.fetchone()['total']
            
            cursor.close()
            conn.close()
            
            response = {
                'success': True,
                'data': trips,
                'total': total_count,
                'limit': limit,
                'offset': offset
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_get_trips: {str(e)}")
            self.send_error(500, f"Error fetching trips: {str(e)}")
    
    def handle_get_statistics(self):
        """GET /api/statistics - Get overall dataset statistics"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trips,
                    AVG(tm.trip_distance_miles) as avg_distance,
                    AVG(tm.avg_speed_mph) as avg_speed,
                    AVG(t.trip_duration) as avg_duration,
                    AVG(t.passenger_count) as avg_passengers,
                    SUM(tm.trip_distance_miles) as total_distance,
                    MIN(t.pickup_datetime) as earliest_trip,
                    MAX(t.pickup_datetime) as latest_trip
                FROM trips t
                INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id
            """)
            
            overall_stats = cursor.fetchone()
            if overall_stats:
                overall_stats['earliest_trip'] = str(overall_stats['earliest_trip'])
                overall_stats['latest_trip'] = str(overall_stats['latest_trip'])
            
            # Vendor statistics
            cursor.execute("""
                SELECT 
                    vendor_id,
                    COUNT(*) as trip_count,
                    AVG(tm.trip_distance_miles) as avg_distance,
                    AVG(tm.avg_speed_mph) as avg_speed
                FROM trips t
                INNER JOIN trip_metrics tm ON t.trip_id = tm.trip_id
                GROUP BY vendor_id
            """)
            
            vendor_stats = cursor.fetchall()
            
            # Time period distribution
            cursor.execute("""
                SELECT 
                    time_period,
                    COUNT(*) as trip_count
                FROM trip_metrics
                GROUP BY time_period
                ORDER BY trip_count DESC
            """)
            
            time_periods = cursor.fetchall()
            
            # Distance category distribution
            cursor.execute("""
                SELECT 
                    distance_category,
                    COUNT(*) as trip_count
                FROM trip_metrics
                GROUP BY distance_category
                ORDER BY FIELD(distance_category, 'short', 'medium', 'long', 'very_long')
            """)
            
            distance_distribution = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            response = {
                'success': True,
                'overall': overall_stats,
                'by_vendor': vendor_stats,
                'by_time_period': time_periods,
                'distance_distribution': distance_distribution
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_get_statistics: {str(e)}")
            self.send_error(500, f"Error fetching statistics: {str(e)}")
    
    def handle_get_insights(self):
        """GET /api/insights - Get analytical insights"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Insight 1: Peak hours analysis
            cursor.execute("""
                SELECT 
                    hour_of_day,
                    COUNT(*) as trip_count,
                    AVG(trip_distance_miles) as avg_distance,
                    AVG(avg_speed_mph) as avg_speed
                FROM trip_metrics
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)
            
            hourly_data = cursor.fetchall()
            
            # Insight 2: Weekend vs Weekday patterns
            cursor.execute("""
                SELECT 
                    is_weekend,
                    COUNT(*) as trip_count,
                    AVG(trip_distance_miles) as avg_distance,
                    AVG(avg_speed_mph) as avg_speed,
                    AVG(trip_efficiency) as avg_efficiency
                FROM trip_metrics
                GROUP BY is_weekend
            """)
            
            weekend_comparison = cursor.fetchall()
            
            # Insight 3: Speed by time period
            cursor.execute("""
                SELECT 
                    time_period,
                    AVG(avg_speed_mph) as avg_speed,
                    COUNT(*) as trip_count
                FROM trip_metrics
                GROUP BY time_period
                ORDER BY avg_speed DESC
            """)
            
            speed_by_period = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            response = {
                'success': True,
                'hourly_patterns': hourly_data,
                'weekend_vs_weekday': weekend_comparison,
                'speed_by_time_period': speed_by_period
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_get_insights: {str(e)}")
            self.send_error(500, f"Error generating insights: {str(e)}")
    
    def handle_hourly_patterns(self):
        """GET /api/hourly-patterns - Get hourly trip patterns"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT 
                    hour_of_day,
                    day_of_week,
                    COUNT(*) as trip_count,
                    AVG(trip_distance_miles) as avg_distance,
                    AVG(avg_speed_mph) as avg_speed
                FROM trip_metrics
                GROUP BY hour_of_day, day_of_week
                ORDER BY day_of_week, hour_of_day
            """)
            
            patterns = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            response = {
                'success': True,
                'data': patterns
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_hourly_patterns: {str(e)}")
            self.send_error(500, f"Error fetching patterns: {str(e)}")
    
    def handle_top_routes(self, params):
        """GET /api/top-routes - Get most frequent routes using custom algorithm"""
        try:
            limit = int(params.get('limit', [10])[0])
            
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Fetch all routes
            cursor.execute("""
                SELECT 
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude
                FROM trips
                WHERE pickup_longitude IS NOT NULL 
                  AND pickup_latitude IS NOT NULL
                  AND dropoff_longitude IS NOT NULL
                  AND dropoff_latitude IS NOT NULL
            """)
            
            routes_data = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            # Use custom algorithm to count route frequencies
            route_counter = RouteFrequencyCounter()
            
            for route in routes_data:
                pickup = (float(route['pickup_longitude']), float(route['pickup_latitude']))
                dropoff = (float(route['dropoff_longitude']), float(route['dropoff_latitude']))
                route_counter.add_route(pickup, dropoff)
            
            # Get top routes
            top_routes = route_counter.get_top_routes(limit)
            
            # Format response
            formatted_routes = []
            for route_key, frequency in top_routes:
                pickup_coords, dropoff_coords = route_key
                formatted_routes.append({
                    'pickup_longitude': pickup_coords[0],
                    'pickup_latitude': pickup_coords[1],
                    'dropoff_longitude': dropoff_coords[0],
                    'dropoff_latitude': dropoff_coords[1],
                    'trip_count': frequency
                })
            
            response = {
                'success': True,
                'data': formatted_routes,
                'total_unique_routes': route_counter.get_total_unique_routes()
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_top_routes: {str(e)}")
            self.send_error(500, f"Error calculating top routes: {str(e)}")
    
    def handle_outliers(self, params):
        """GET /api/outliers - Detect outliers using custom algorithm"""
        try:
            metric = params.get('metric', ['speed'])[0]  # speed, distance, or duration
            
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Fetch metric data
            if metric == 'speed':
                cursor.execute("SELECT avg_speed_mph as value FROM trip_metrics WHERE avg_speed_mph IS NOT NULL")
            elif metric == 'distance':
                cursor.execute("SELECT trip_distance_miles as value FROM trip_metrics WHERE trip_distance_miles IS NOT NULL")
            elif metric == 'duration':
                cursor.execute("SELECT trip_duration as value FROM trips WHERE trip_duration IS NOT NULL")
            else:
                cursor.close()
                conn.close()
                self.send_error(400, "Invalid metric. Use: speed, distance, or duration")
                return
            
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            
            # Extract values and convert Decimal to float
            values = [float(row['value']) for row in data if row['value'] is not None]
            
            if not values:
                response = {
                    'success': False,
                    'error': 'No data available for this metric'
                }
                self._send_json_response(response)
                return
            
            # Use custom outlier detection algorithm
            detector = OutlierDetector()
            outlier_info = detector.detect_outliers(values, multiplier=1.5)
            basic_stats = detector.calculate_statistics(values)
            
            response = {
                'success': True,
                'metric': metric,
                'outlier_analysis': outlier_info,
                'statistics': basic_stats
            }
            
            self._send_json_response(response)
            
        except Exception as e:
            print(f"Error in handle_outliers: {str(e)}")
            self.send_error(500, f"Error detecting outliers: {str(e)}")
    
    def log_message(self, format, *args):
        """Override to customize logging"""
        print(f"[{self.log_date_time_string()}] {format % args}")


def run_server():
    """Start the HTTP server"""
    try:
        with socketserver.TCPServer((SERVER_HOST, SERVER_PORT), TaxiAPIHandler) as httpd:
            print("="*60)
            print("NYC TAXI DATA EXPLORER - Backend Server")
            print("="*60)
            print(f"Server running on http://{SERVER_HOST}:{SERVER_PORT}")
            print(f"Frontend: http://{SERVER_HOST}:{SERVER_PORT}")
            print(f"API Base: http://{SERVER_HOST}:{SERVER_PORT}/api")
            print("\nAvailable Endpoints:")
            print("  GET  /api/trips          - Fetch trip data with filters")
            print("  GET  /api/statistics     - Overall statistics")
            print("  GET  /api/insights       - Analytical insights")
            print("  GET  /api/hourly-patterns - Hourly trip patterns")
            print("  GET  /api/top-routes     - Most frequent routes")
            print("  GET  /api/outliers       - Outlier detection")
            print("\nPress Ctrl+C to stop the server")
            print("="*60 + "\n")
            
            httpd.serve_forever()
            
    except KeyboardInterrupt:
        print("\n\nServer stopped by user")
    except Exception as e:
        print(f"\nError starting server: {e}")


if __name__ == "__main__":
    run_server()