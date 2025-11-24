import React, { createContext, useState, useContext, useEffect } from 'react';
import api from '../services/api';

const AuthContext = createContext();

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check if user is already logged in
    const token = localStorage.getItem('token');
    const savedUser = localStorage.getItem('user');
    
    if (token && savedUser) {
      setUser(JSON.parse(savedUser));
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
    }
    setLoading(false);
  }, []);

  const login = async (email, password) => {
    try {
      // TODO: Replace with actual API call when backend is ready
      // const response = await api.post('/auth/login', { email, password });
      
      // MOCK LOGIN - Remove this when backend is ready
      console.log('Mock login with:', email, password);
      
      const mockResponse = {
        data: {
          token: 'mock-jwt-token-' + Date.now(),
          user: {
            id: '1',
            name: email.split('@')[0],
            email: email,
            role: 'admin'
          }
        }
      };
      
      const { token, user } = mockResponse.data;
      
      localStorage.setItem('token', token);
      localStorage.setItem('user', JSON.stringify(user));
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
      setUser(user);
      
      return { success: true };
    } catch (error) {
      console.error('Login error:', error);
      return { 
        success: false, 
        error: error.response?.data?.message || 'Login failed. Please try again.' 
      };
    }
  };

  const signup = async (name, email, password, role = 'analyst') => {
    try {
      // TODO: Replace with actual API call when backend is ready
      // const response = await api.post('/auth/signup', { name, email, password, role });
      
      // MOCK SIGNUP - Remove this when backend is ready
      console.log('Mock signup with:', name, email, role);
      
      const mockResponse = {
        data: {
          token: 'mock-jwt-token-' + Date.now(),
          user: {
            id: Date.now().toString(),
            name: name,
            email: email,
            role: role
          }
        }
      };
      
      const { token, user } = mockResponse.data;
      
      localStorage.setItem('token', token);
      localStorage.setItem('user', JSON.stringify(user));
      api.defaults.headers.common['Authorization'] = `Bearer ${token}`;
      setUser(user);
      
      return { success: true };
    } catch (error) {
      console.error('Signup error:', error);
      return { 
        success: false, 
        error: error.response?.data?.message || 'Signup failed. Please try again.' 
      };
    }
  };

  const logout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    delete api.defaults.headers.common['Authorization'];
    setUser(null);
  };

  const value = {
    user,
    login,
    signup,
    logout,
    isAuthenticated: !!user,
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};