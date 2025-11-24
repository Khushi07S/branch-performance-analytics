import React, { useState, useEffect } from 'react';
import { Building2, TrendingUp, Users, CreditCard } from 'lucide-react';
import Header from '../Layout/Header';
import KPICard from './KPICard';
import TransactionChart from './TransactionChart';
import BranchTable from './BranchTable';
import api from '../../services/api';

const Dashboard = () => {
  const [kpiData, setKpiData] = useState({
    totalBranches: 0,
    totalDeposits: 0,
    totalCredits: 0,
    totalCustomers: 0,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      // Uncomment when backend is ready
      // const response = await api.get('/dashboard/kpis');
      // setKpiData(response.data);
      
      // Sample data for now
      setKpiData({
        totalBranches: 25,
        totalDeposits: 125000000,
        totalCredits: 89000000,
        totalCustomers: 15420,
      });
      
      setLoading(false);
    } catch (err) {
      console.error('Error fetching dashboard data:', err);
      setError('Failed to load dashboard data');
      setLoading(false);
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
      notation: 'compact',
      compactDisplay: 'short',
    }).format(value);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <p className="text-red-600 text-lg font-semibold">{error}</p>
          <button
            onClick={fetchDashboardData}
            className="mt-4 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <Header 
        title="Dashboard" 
        subtitle="Welcome back! Here's an overview of your branch performance."
      />

      {/* Main Content */}
      <div className="ml-64 pt-16">
        <div className="p-8">
          {/* KPI Cards Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <KPICard
              title="Total Branches"
              value={kpiData.totalBranches}
              change="+2.5%"
              trend="up"
              icon={Building2}
              color="blue"
            />
            <KPICard
              title="Total Deposits"
              value={formatCurrency(kpiData.totalDeposits)}
              change="+12.3%"
              trend="up"
              icon={TrendingUp}
              color="green"
            />
            <KPICard
              title="Total Credits"
              value={formatCurrency(kpiData.totalCredits)}
              change="+8.7%"
              trend="up"
              icon={CreditCard}
              color="purple"
            />
            <KPICard
              title="Total Customers"
              value={kpiData.totalCustomers.toLocaleString()}
              change="+5.2%"
              trend="up"
              icon={Users}
              color="orange"
            />
          </div>

          {/* Charts Section */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <TransactionChart title="Monthly Deposits vs Credits" />
            
            {/* Additional Chart - Pie/Donut Chart Placeholder */}
            <div className="bg-white rounded-lg border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">
                Branch Distribution by Region
              </h3>
              <div className="flex items-center justify-center h-80 text-gray-400">
                <div className="text-center">
                  <Building2 className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p>Regional distribution chart will appear here</p>
                  <p className="text-sm mt-2">Connected to backend API</p>
                </div>
              </div>
            </div>
          </div>

          {/* Branch Performance Table */}
          <BranchTable />
        </div>
      </div>
    </div>
  );
};

export default Dashboard;