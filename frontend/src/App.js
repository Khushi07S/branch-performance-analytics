import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';
import ProtectedRoute from './components/ProtectedRoute';
import Login from './components/Auth/Login';
import Signup from './components/Auth/Signup';
import Dashboard from './components/Dashboard/Dashboard';
import Sidebar from './components/Layout/Sidebar';
import './App.css';

function App() {
  return (
    <AuthProvider>
      <Router>
        <Routes>
          {/* Public Routes */}
          <Route path="/login" element={<Login />} />
          <Route path="/signup" element={<Signup />} />

          {/* Protected Routes */}
          <Route
            path="/dashboard"
            element={
              <ProtectedRoute>
                <div className="flex">
                  <Sidebar />
                  <Dashboard />
                </div>
              </ProtectedRoute>
            }
          />

          {/* Placeholder routes for other pages */}
          <Route
            path="/branches"
            element={
              <ProtectedRoute>
                <div className="flex">
                  <Sidebar />
                  <div className="ml-64 pt-16 p-8 w-full">
                    <h1 className="text-2xl font-bold">Branches Page</h1>
                    <p className="text-gray-600 mt-2">Branch management coming soon...</p>
                  </div>
                </div>
              </ProtectedRoute>
            }
          />

          <Route
            path="/analytics"
            element={
              <ProtectedRoute>
                <div className="flex">
                  <Sidebar />
                  <div className="ml-64 pt-16 p-8 w-full">
                    <h1 className="text-2xl font-bold">Analytics Page</h1>
                    <p className="text-gray-600 mt-2">Advanced analytics coming soon...</p>
                  </div>
                </div>
              </ProtectedRoute>
            }
          />

          <Route
            path="/reports"
            element={
              <ProtectedRoute>
                <div className="flex">
                  <Sidebar />
                  <div className="ml-64 pt-16 p-8 w-full">
                    <h1 className="text-2xl font-bold">Reports Page</h1>
                    <p className="text-gray-600 mt-2">Report generation coming soon...</p>
                  </div>
                </div>
              </ProtectedRoute>
            }
          />

          <Route
            path="/settings"
            element={
              <ProtectedRoute>
                <div className="flex">
                  <Sidebar />
                  <div className="ml-64 pt-16 p-8 w-full">
                    <h1 className="text-2xl font-bold">Settings Page</h1>
                    <p className="text-gray-600 mt-2">System settings coming soon...</p>
                  </div>
                </div>
              </ProtectedRoute>
            }
          />

          {/* Default redirect */}
          <Route path="/" element={<Navigate to="/login" replace />} />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </Router>
    </AuthProvider>
  );
}

export default App;