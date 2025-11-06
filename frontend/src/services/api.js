import axios from 'axios';

// Create axios instance with base configuration
const api = axios.create({
  baseURL: process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000/api',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor - adds JWT token to all requests
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor - handles common errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Token expired or invalid - logout user
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// ============================================
// AUTH APIs
// ============================================

export const authAPI = {
  // Login user
  login: async (credentials) => {
    const response = await api.post('/auth/login', credentials);
    return response.data;
  },

  // Register new user
  signup: async (userData) => {
    const response = await api.post('/auth/signup', userData);
    return response.data;
  },

  // Logout
  logout: () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
  },
};

// ============================================
// DASHBOARD APIs
// ============================================

export const dashboardAPI = {
  // Get overall KPIs
  getOverallKPIs: async () => {
    const response = await api.get('/dashboard/kpis');
    return response.data;
  },

  // Get monthly transaction trends
  getTransactionTrends: async (params = {}) => {
    const response = await api.get('/dashboard/trends', { params });
    return response.data;
  },

  // Get device category breakdown
  getDeviceStats: async () => {
    const response = await api.get('/dashboard/device-stats');
    return response.data;
  },
};

// ============================================
// BRANCH APIs
// ============================================

export const branchAPI = {
  // Get all branches
  getAllBranches: async (params = {}) => {
    const response = await api.get('/branches', { params });
    return response.data;
  },

  // Get single branch details
  getBranchById: async (branchId) => {
    const response = await api.get(`/branches/${branchId}`);
    return response.data;
  },

  // Get branch KPIs
  getBranchKPIs: async (branchId) => {
    const response = await api.get(`/branches/${branchId}/kpis`);
    return response.data;
  },

  // Get branch locations for map
  getBranchLocations: async () => {
    const response = await api.get('/branches/locations');
    return response.data;
  },
};

// ============================================
// EXPORT APIs
// ============================================

export const exportAPI = {
  // Export data as CSV
  exportCSV: async (params = {}) => {
    const response = await api.get('/export/csv', {
      params,
      responseType: 'blob',
    });
    return response.data;
  },

  // Export data as PDF
  exportPDF: async (params = {}) => {
    const response = await api.get('/export/pdf', {
      params,
      responseType: 'blob',
    });
    return response.data;
  },
};

// ============================================
// HELPER FUNCTIONS
// ============================================

// Download file helper
export const downloadFile = (blob, filename) => {
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.URL.revokeObjectURL(url);
};

export default api;