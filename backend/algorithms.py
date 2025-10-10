class QuickSort:
    
    @staticmethod
    def sort(arr, key=None, reverse=False):
        if not arr:
            return []
        
        # Create a copy to avoid modifying original
        result = arr[:]
        
        def get_value(item):
            """Extract sorting value from item"""
            if key:
                return key(item)
            return item
        
        def partition(low, high):
            """
            Lomuto partition scheme
            Places pivot element at its correct position
            """
            # Choose last element as pivot
            pivot_val = get_value(result[high])
            i = low - 1
            
            for j in range(low, high):
                current_val = get_value(result[j])
                
                # Comparison based on sort order
                if reverse:
                    should_swap = current_val > pivot_val
                else:
                    should_swap = current_val < pivot_val
                
                if should_swap:
                    i += 1
                    result[i], result[j] = result[j], result[i]
            
            result[i + 1], result[high] = result[high], result[i + 1]
            return i + 1
        
        def quicksort_recursive(low, high):
            """Recursive QuickSort implementation"""
            if low < high:
                # Partition and get pivot index
                pi = partition(low, high)
                
                # Recursively sort elements before and after partition
                quicksort_recursive(low, pi - 1)
                quicksort_recursive(pi + 1, high)
        
        quicksort_recursive(0, len(result) - 1)
        return result


class RouteFrequencyCounter:
    
    def __init__(self):
        self.frequency_map = {}
    
    def add_route(self, pickup_coords, dropoff_coords):
        # Round coordinates to reduce granularity (group nearby locations)
        pickup_rounded = (round(pickup_coords[0], 3), round(pickup_coords[1], 3))
        dropoff_rounded = (round(dropoff_coords[0], 3), round(dropoff_coords[1], 3))
        
        route_key = (pickup_rounded, dropoff_rounded)
        
        if route_key in self.frequency_map:
            self.frequency_map[route_key] += 1
        else:
            self.frequency_map[route_key] = 1
    
    def get_top_routes(self, n=10):
        if not self.frequency_map:
            return []
        
        # Convert to list of tuples
        routes_list = []
        for route, freq in self.frequency_map.items():
            routes_list.append((route, freq))
        
        # Manual selection sort to find top N
        # We only need top N, so we can optimize by doing N passes
        result = []
        
        for _ in range(min(n, len(routes_list))):
            if not routes_list:
                break
            
            # Find maximum frequency in remaining routes
            max_idx = 0
            max_freq = routes_list[0][1]
            
            for i in range(1, len(routes_list)):
                if routes_list[i][1] > max_freq:
                    max_freq = routes_list[i][1]
                    max_idx = i
            
            # Add to result and remove from list
            result.append(routes_list[max_idx])
            routes_list.pop(max_idx)
        
        return result
    
    def get_total_unique_routes(self):
        """Get count of unique routes"""
        return len(self.frequency_map)


class OutlierDetector:
    
    @staticmethod
    def calculate_quartiles(data):
        if not data:
            return None, None, None
        
        # Sort data manually using our QuickSort
        sorted_data = QuickSort.sort(data)
        n = len(sorted_data)
        
        # Calculate median (Q2)
        if n % 2 == 0:
            q2 = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        else:
            q2 = sorted_data[n // 2]
        
        # Calculate Q1 (median of lower half)
        lower_half = sorted_data[:n // 2]
        if len(lower_half) % 2 == 0:
            q1 = (lower_half[len(lower_half) // 2 - 1] + lower_half[len(lower_half) // 2]) / 2
        else:
            q1 = lower_half[len(lower_half) // 2]
        
        # Calculate Q3 (median of upper half)
        if n % 2 == 0:
            upper_half = sorted_data[n // 2:]
        else:
            upper_half = sorted_data[n // 2 + 1:]
        
        if len(upper_half) % 2 == 0:
            q3 = (upper_half[len(upper_half) // 2 - 1] + upper_half[len(upper_half) // 2]) / 2
        else:
            q3 = upper_half[len(upper_half) // 2]
        
        return q1, q2, q3
    
    @staticmethod
    def detect_outliers(data, multiplier=1.5):
        """
        Detect outliers using IQR method
        
        Args:
            data: List of numerical values
            multiplier: IQR multiplier (typically 1.5 for outliers, 3.0 for extreme outliers)
        
        Returns:
            Dictionary with outlier information
        """
        if not data or len(data) < 4:
            return {
                'outliers': [],
                'lower_bound': None,
                'upper_bound': None,
                'q1': None,
                'q2': None,
                'q3': None,
                'iqr': None
            }
        
        q1, q2, q3 = OutlierDetector.calculate_quartiles(data)
        iqr = q3 - q1
        
        # Calculate bounds
        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)
        
        # Find outliers
        outliers = []
        for value in data:
            if value < lower_bound or value > upper_bound:
                outliers.append(value)
        
        return {
            'outliers': outliers,
            'outlier_count': len(outliers),
            'outlier_percentage': (len(outliers) / len(data)) * 100,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'q1': q1,
            'q2': q2,
            'q3': q3,
            'iqr': iqr,
            'total_values': len(data)
        }
    
    @staticmethod
    def calculate_statistics(data):

        if not data:
            return {
                'mean': None,
                'median': None,
                'min': None,
                'max': None,
                'range': None,
                'std_dev': None
            }
        
        n = len(data)
        
        # Calculate mean
        total = 0
        for value in data:
            total += value
        mean = total / n
        
        # Calculate median
        sorted_data = QuickSort.sort(data)
        if n % 2 == 0:
            median = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        else:
            median = sorted_data[n // 2]
        
        # Calculate min and max
        min_val = sorted_data[0]
        max_val = sorted_data[-1]
        
        # Calculate standard deviation
        variance_sum = 0
        for value in data:
            variance_sum += (value - mean) ** 2
        variance = variance_sum / n
        std_dev = variance ** 0.5
        
        return {
            'mean': mean,
            'median': median,
            'min': min_val,
            'max': max_val,
            'range': max_val - min_val,
            'std_dev': std_dev,
            'count': n
        }


class TimeSeriesGrouper:

    
    def __init__(self):
        self.groups = {}
    
    def add_to_group(self, key, value):

        if key not in self.groups:
            self.groups[key] = []
        self.groups[key].append(value)
    
    def aggregate(self, metric_keys):

        result = {}
        
        for group_key, values in self.groups.items():
            if not values:
                continue
            
            group_result = {
                'count': len(values)
            }
            
            # Calculate aggregates for each metric
            for metric in metric_keys:
                metric_values = []
                for item in values:
                    if metric in item and item[metric] is not None:
                        metric_values.append(item[metric])
                
                if metric_values:
                    # Calculate sum
                    total = 0
                    for val in metric_values:
                        total += val
                    
                    # Calculate average
                    avg = total / len(metric_values)
                    
                    # Find min and max
                    min_val = metric_values[0]
                    max_val = metric_values[0]
                    for val in metric_values[1:]:
                        if val < min_val:
                            min_val = val
                        if val > max_val:
                            max_val = val
                    
                    group_result[f'{metric}_sum'] = total
                    group_result[f'{metric}_avg'] = avg
                    group_result[f'{metric}_min'] = min_val
                    group_result[f'{metric}_max'] = max_val
            
            result[group_key] = group_result
        
        return result
    
    def get_sorted_groups(self, sort_by='count', reverse=True):

        # First aggregate
        aggregated = self.aggregate([])
        
        # Convert to list
        groups_list = []
        for key, metrics in aggregated.items():
            groups_list.append((key, metrics))
        
        # Sort using our QuickSort
        if sort_by in ['count'] and groups_list:
            sorted_groups = QuickSort.sort(
                groups_list,
                key=lambda x: x[1].get(sort_by, 0),
                reverse=reverse
            )
            return sorted_groups
        
        return groups_list


# Pseudo-code and Complexity Analysis Documentation
"""
ALGORITHM 1: QUICKSORT
======================
Pseudo-code:
    function quicksort(arr, low, high):
        if low < high:
            pivot_index = partition(arr, low, high)
            quicksort(arr, low, pivot_index - 1)
            quicksort(arr, pivot_index + 1, high)
    
    function partition(arr, low, high):
        pivot = arr[high]
        i = low - 1
        for j from low to high - 1:
            if arr[j] < pivot:
                i = i + 1
                swap arr[i] with arr[j]
        swap arr[i + 1] with arr[high]
        return i + 1

Time Complexity:
    - Best/Average Case: O(n log n)
        * Partition divides array in half each time
        * log n levels of recursion, n work per level
    - Worst Case: O(n²)
        * Occurs when pivot is always smallest/largest element
        * n levels of recursion, O(n) work per level
    
Space Complexity: O(log n)
    - Recursion stack depth in balanced case
    - O(n) in worst case (unbalanced recursion)


ALGORITHM 2: ROUTE FREQUENCY COUNTER
=====================================
Pseudo-code:
    function add_route(pickup, dropoff):
        route_key = create_key(pickup, dropoff)
        if route_key in frequency_map:
            frequency_map[route_key] += 1
        else:
            frequency_map[route_key] = 1
    
    function get_top_routes(n):
        result = []
        for i from 0 to n:
            max_route = find_max(frequency_map)
            result.append(max_route)
            remove max_route from frequency_map
        return result

Time Complexity:
    - add_route: O(1) average (hash table insert)
    - get_top_routes: O(n * m) where n is top count, m is unique routes
    
Space Complexity: O(u)
    - Where u is number of unique routes
    - Hash table stores one entry per unique route


ALGORITHM 3: OUTLIER DETECTION (IQR)
=====================================
Pseudo-code:
    function detect_outliers(data, multiplier):
        sorted_data = sort(data)
        q1 = calculate_quartile(sorted_data, 0.25)
        q3 = calculate_quartile(sorted_data, 0.75)
        iqr = q3 - q1
        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)
        
        outliers = []
        for value in data:
            if value < lower_bound or value > upper_bound:
                outliers.append(value)
        return outliers

Time Complexity:
    - O(n log n) for sorting
    - O(n) for finding outliers
    - Overall: O(n log n)
    
Space Complexity: O(n)
    - Stores sorted copy of data
    - Stores outlier list
"""


# Test functions to verify implementations
if __name__ == "__main__":
    print("Testing Custom Algorithms...")
    print("=" * 60)
    
    # Test QuickSort
    print("\n1. Testing QuickSort:")
    test_arr = [64, 34, 25, 12, 22, 11, 90]
    sorted_arr = QuickSort.sort(test_arr)
    print(f"   Original: {test_arr}")
    print(f"   Sorted:   {sorted_arr}")
    print(f"   Reverse:  {QuickSort.sort(test_arr, reverse=True)}")
    
    # Test Route Counter
    print("\n2. Testing Route Frequency Counter:")
    counter = RouteFrequencyCounter()
    routes = [
        ((-73.98, 40.76), (-73.96, 40.77)),
        ((-73.98, 40.76), (-73.96, 40.77)),
        ((-73.99, 40.75), (-73.97, 40.76)),
        ((-73.98, 40.76), (-73.96, 40.77)),
    ]
    for pickup, dropoff in routes:
        counter.add_route(pickup, dropoff)
    top = counter.get_top_routes(2)
    print(f"   Top routes: {top}")
    
    # Test Outlier Detection
    print("\n3. Testing Outlier Detection:")
    data = [10, 12, 12, 13, 12, 11, 14, 13, 15, 10, 10, 100, 12]
    result = OutlierDetector.detect_outliers(data)
    print(f"   Data: {data}")
    print(f"   Outliers found: {result['outlier_count']}")
    print(f"   Outlier values: {result['outliers']}")
    print(f"   Q1: {result['q1']:.2f}, Q3: {result['q3']:.2f}")
    
    print("\n" + "=" * 60)
    print("All tests completed successfully!")