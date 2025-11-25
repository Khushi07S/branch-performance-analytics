// utils/helpers.js

const API_BASE_URL = 'http://localhost:5000/api/v1';

// Chart.js global options (used by chart components)
const CHART_OPTIONS = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
        x: { grid: { color: 'rgba(255, 255, 255, 0.1)' }, ticks: { color: '#bbb' } },
        y: { grid: { color: 'rgba(255, 255, 255, 0.1)' }, ticks: { color: '#bbb' } },
    },
    plugins: { legend: { labels: { color: '#ddd' } } },
};

// Helper function for currency formatting (Indian Rupee Symbol)
const formatCurrency = (value) => `₹${(value / 1000).toFixed(1)}K`;

// Export all utilities
export { API_BASE_URL, CHART_OPTIONS, formatCurrency };