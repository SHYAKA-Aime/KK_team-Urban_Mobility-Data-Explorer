// API Configuration
const API_BASE = 'http://localhost:8000/api';

// Application State
const state = {
    currentPage: 1,
    pageSize: 50,
    totalTrips: 0,
    filters: {},
    charts: {}
};

// Initialize application
document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
    setupEventListeners();
});

async function initializeApp() {
    showLoading(true);
    
    try {
        // Populate hour filter
        populateHourFilter();
        
        // Load initial data
        await Promise.all([
            loadStatistics(),
            loadInsights(),
            loadTrips(),
            loadTopRoutes()
        ]);
        
    } catch (error) {
        console.error('Initialization error:', error);
        alert('Failed to load initial data. Please refresh the page.');
    } finally {
        showLoading(false);
    }
}

function populateHourFilter() {
    const hourFilter = document.getElementById('hourFilter');
    for (let i = 0; i < 24; i++) {
        const option = document.createElement('option');
        option.value = i;
        option.textContent = `${i}:00 - ${i}:59`;
        hourFilter.appendChild(option);
    }
}

function setupEventListeners() {
    // Filter buttons
    document.getElementById('applyFilters').addEventListener('click', applyFilters);
    document.getElementById('resetFilters').addEventListener('click', resetFilters);
    
    // Pagination
    document.getElementById('prevPage').addEventListener('click', () => changePage(-1));
    document.getElementById('nextPage').addEventListener('click', () => changePage(1));
}

// API Functions
async function fetchAPI(endpoint, params = {}) {
    const queryString = new URLSearchParams(params).toString();
    const url = `${API_BASE}${endpoint}${queryString ? '?' + queryString : ''}`;
    
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`API error: ${response.statusText}`);
    }
    
    return await response.json();
}

async function loadStatistics() {
    try {
        const data = await fetchAPI('/statistics');
        
        if (data.success) {
            displayStatistics(data.overall);
            createVendorChart(data.by_vendor);
            createDistributionChart(data.distance_distribution);
        }
    } catch (error) {
        console.error('Error loading statistics:', error);
    }
}

async function loadInsights() {
    try {
        const data = await fetchAPI('/insights');
        
        if (data.success) {
            displayInsights(data);
            createHourlyChart(data.hourly_patterns);
            createDayOfWeekChart(data.hourly_patterns);
            createSpeedChart(data.speed_by_time_period);
        }
    } catch (error) {
        console.error('Error loading insights:', error);
    }
}

async function loadTrips() {
    try {
        const params = {
            limit: state.pageSize,
            offset: (state.currentPage - 1) * state.pageSize,
            ...state.filters
        };
        
        const data = await fetchAPI('/trips', params);
        
        if (data.success) {
            displayTrips(data.data);
            updatePagination(data.total);
            state.totalTrips = data.total;
        }
    } catch (error) {
        console.error('Error loading trips:', error);
        document.getElementById('tripsTableBody').innerHTML = 
            '<tr><td colspan="8" class="loading">Error loading trips</td></tr>';
    }
}

async function loadTopRoutes() {
    try {
        const data = await fetchAPI('/top-routes', { limit: 10 });
        
        if (data.success) {
            displayTopRoutes(data.data);
        }
    } catch (error) {
        console.error('Error loading top routes:', error);
    }
}

// Display Functions
function displayStatistics(stats) {
    document.getElementById('totalTrips').textContent = 
        stats.total_trips.toLocaleString();
    
    document.getElementById('avgDistance').textContent = 
        `${stats.avg_distance.toFixed(2)} miles`;
    
    document.getElementById('avgSpeed').textContent = 
        `${stats.avg_speed.toFixed(2)} mph`;
    
    document.getElementById('avgDuration').textContent = 
        `${(stats.avg_duration / 60).toFixed(1)} min`;
}

function displayTrips(trips) {
    const tbody = document.getElementById('tripsTableBody');
    
    if (trips.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No trips found</td></tr>';
        return;
    }
    
    tbody.innerHTML = trips.map(trip => `
        <tr>
            <td>${trip.trip_id}</td>
            <td>${trip.vendor_id}</td>
            <td>${formatDateTime(trip.pickup_datetime)}</td>
            <td>${(trip.trip_duration / 60).toFixed(1)}</td>
            <td>${trip.trip_distance_miles.toFixed(2)}</td>
            <td>${trip.avg_speed_mph.toFixed(2)}</td>
            <td>${trip.passenger_count}</td>
            <td>${trip.time_period.replace('_', ' ')}</td>
        </tr>
    `).join('');
}



function displayTopRoutes(routes) {
    const container = document.getElementById('topRoutes');
    
    if (routes.length === 0) {
        container.innerHTML = '<p class="loading">No routes data available</p>';
        return;
    }
    
    container.innerHTML = routes.map((route, index) => `
        <div class="route-card">
            <h4>Route ${index + 1}</h4>
            <div class="route-info">
                Pickup: (${route.pickup_latitude.toFixed(4)}, ${route.pickup_longitude.toFixed(4)})
            </div>
            <div class="route-info">
                Dropoff: (${route.dropoff_latitude.toFixed(4)}, ${route.dropoff_longitude.toFixed(4)})
            </div>
            <div class="route-count">${route.trip_count} trips</div>
        </div>
    `).join('');
}

// Chart Functions
function createHourlyChart(data) {
    // Group by hour
    const hourlyStats = {};
    
    data.forEach(item => {
        const hour = item.hour_of_day;
        if (!hourlyStats[hour]) {
            hourlyStats[hour] = {
                count: 0,
                totalDistance: 0,
                totalSpeed: 0
            };
        }
        hourlyStats[hour].count += item.trip_count;
        hourlyStats[hour].totalDistance += item.avg_distance * item.trip_count;
        hourlyStats[hour].totalSpeed += item.avg_speed * item.trip_count;
    });
    
    const hours = Object.keys(hourlyStats).sort((a, b) => a - b);
    const counts = hours.map(h => hourlyStats[h].count);
    
    const canvas = document.getElementById('hourlyChart');
    const ctx = canvas.getContext('2d');
    
    drawBarChart(ctx, canvas, {
        labels: hours.map(h => `${h}:00`),
        data: counts,
        color: '#667eea',
        title: 'Trip Count by Hour'
    });
}

function createDayOfWeekChart(data) {
    // Group by day of week
    const dayStats = {};
    const dayNames = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
    
    data.forEach(item => {
        const day = item.day_of_week;
        if (!dayStats[day]) {
            dayStats[day] = 0;
        }
        dayStats[day] += item.trip_count;
    });
    
    const days = Object.keys(dayStats).sort((a, b) => a - b);
    const counts = days.map(d => dayStats[d]);
    const labels = days.map(d => dayNames[d]);
    
    const canvas = document.getElementById('dayOfWeekChart');
    const ctx = canvas.getContext('2d');
    
    drawBarChart(ctx, canvas, {
        labels: labels,
        data: counts,
        color: '#764ba2',
        title: 'Trip Count by Day'
    });
}

function createVendorChart(vendors) {
    // This would create a vendor comparison chart
    // Simplified for now
}

function createDistributionChart(distribution) {
    const canvas = document.getElementById('distanceChart');
    const ctx = canvas.getContext('2d');
    
    const labels = distribution.map(d => d.distance_category);
    const data = distribution.map(d => d.trip_count);
    
    drawBarChart(ctx, canvas, {
        labels: labels,
        data: data,
        color: '#667eea',
        title: 'Distance Distribution'
    });
}

function createSpeedChart(speedData) {
    const canvas = document.getElementById('speedChart');
    const ctx = canvas.getContext('2d');
    
    const labels = speedData.map(d => d.time_period.replace('_', ' '));
    const data = speedData.map(d => d.avg_speed);
    
    drawBarChart(ctx, canvas, {
        labels: labels,
        data: data,
        color: '#764ba2',
        title: 'Average Speed by Time Period'
    });
}

// Simple bar chart drawer (no external libraries)
function drawBarChart(ctx, canvas, options) {
    const { labels, data, color, title } = options;
    const padding = 50;
    const width = canvas.width = canvas.offsetWidth;
    const height = canvas.height = 300;
    
    ctx.clearRect(0, 0, width, height);
    
    const maxValue = Math.max(...data);
    const barWidth = (width - 2 * padding) / data.length;
    const scale = (height - 2 * padding) / maxValue;
    
    // Draw bars
    data.forEach((value, index) => {
        const barHeight = value * scale;
        const x = padding + index * barWidth;
        const y = height - padding - barHeight;
        
        ctx.fillStyle = color;
        ctx.fillRect(x + 5, y, barWidth - 10, barHeight);
        
        // Draw value on top
        ctx.fillStyle = '#333';
        ctx.font = '12px Arial';
        ctx.textAlign = 'center';
        ctx.fillText(Math.round(value), x + barWidth / 2, y - 5);
        
        // Draw label
        ctx.save();
        ctx.translate(x + barWidth / 2, height - padding + 20);
        ctx.rotate(-Math.PI / 4);
        ctx.textAlign = 'right';
        ctx.fillText(labels[index], 0, 0);
        ctx.restore();
    });
    
    // Draw axes
    ctx.strokeStyle = '#333';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(padding, padding);
    ctx.lineTo(padding, height - padding);
    ctx.lineTo(width - padding, height - padding);
    ctx.stroke();
}

// Filter Functions
function applyFilters() {
    const filters = {};
    
    const vendor = document.getElementById('vendorFilter').value;
    const hour = document.getElementById('hourFilter').value;
    const day = document.getElementById('dayFilter').value;
    const weekend = document.getElementById('weekendFilter').value;
    const minDistance = document.getElementById('minDistance').value;
    const maxDistance = document.getElementById('maxDistance').value;
    const minDuration = document.getElementById('minDuration').value;
    const maxDuration = document.getElementById('maxDuration').value;
    const sortBy = document.getElementById('sortBy').value;
    const sortOrder = document.getElementById('sortOrder').value;
    
    if (vendor) filters.vendor_id = vendor;
    if (hour) filters.hour = hour;
    if (day) filters.day_of_week = day;
    if (weekend) filters.is_weekend = weekend;
    if (minDistance) filters.min_distance = minDistance;
    if (maxDistance) filters.max_distance = maxDistance;
    if (minDuration) filters.min_duration = minDuration;
    if (maxDuration) filters.max_duration = maxDuration;
    if (sortBy) filters.sort_by = sortBy;
    if (sortOrder) filters.order = sortOrder;
    
    state.filters = filters;
    state.currentPage = 1;
    
    loadTrips();
}

function resetFilters() {
    document.getElementById('vendorFilter').value = '';
    document.getElementById('hourFilter').value = '';
    document.getElementById('dayFilter').value = '';
    document.getElementById('weekendFilter').value = '';
    document.getElementById('minDistance').value = '';
    document.getElementById('maxDistance').value = '';
    document.getElementById('minDuration').value = '';
    document.getElementById('maxDuration').value = '';
    document.getElementById('sortBy').value = 'pickup_datetime';
    document.getElementById('sortOrder').value = 'desc';
    
    state.filters = {};
    state.currentPage = 1;
    
    loadTrips();
}

// Pagination Functions
function updatePagination(total) {
    const totalPages = Math.ceil(total / state.pageSize);
    const start = (state.currentPage - 1) * state.pageSize + 1;
    const end = Math.min(state.currentPage * state.pageSize, total);
    
    document.getElementById('paginationInfo').textContent = 
        `Showing ${start}-${end} of ${total.toLocaleString()} trips`;
    
    document.getElementById('pageInfo').textContent = 
        `Page ${state.currentPage} of ${totalPages}`;
    
    document.getElementById('prevPage').disabled = state.currentPage === 1;
    document.getElementById('nextPage').disabled = state.currentPage === totalPages;
}

function changePage(delta) {
    state.currentPage += delta;
    loadTrips();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// Utility Functions
function formatDateTime(dateTimeStr) {
    const date = new Date(dateTimeStr);
    return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function showLoading(show) {
    const overlay = document.getElementById('loadingOverlay');
    if (show) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

// Export for potential testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        formatDateTime,
        applyFilters,
        resetFilters
    };
}