// components/DataDisplay.jsx

import React from 'react';
// import { formatCurrency } from '../utils/helpers'; // Assumed imports

const formatCurrency = (value) => `₹${(value / 1000).toFixed(1)}K`; // Helper included locally for context

export const KpiCard = ({ title, value, subtext, color }) => (
    <div className={`p-6 rounded-xl shadow-lg ${color} bg-opacity-70 border-b-4 border-opacity-90`}>
        <p className="text-sm font-medium text-gray-200 mb-1">{title}</p>
        <h2 className="text-3xl font-extrabold text-white mb-2">{value}</h2>
        <p className="text-xs text-gray-300">{subtext}</p>
    </div>
);

export const DetailedRegionalTable = ({ data }) => (
    <div className="bg-gray-800 p-6 rounded-xl shadow-2xl overflow-x-auto">
        <h3 className="text-xl font-bold text-white mb-4">Detailed Regional Data Table</h3>
        <table className="min-w-full divide-y divide-gray-700">
            <thead className="bg-gray-700">
                <tr>
                    {['Region', 'State', 'Branches', 'Deposits (K)', 'Credit (K)', 'Ratio D/C'].map(header => (
                        <th key={header} className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                            {header}
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody className="divide-y divide-gray-800">
                {data.map((row, index) => (
                    <tr key={index} className="hover:bg-gray-700 transition duration-150">
                        <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-teal-400">{row.region}</td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-300">{row.state}</td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-300">{row.branch_count}</td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-white">{formatCurrency(row.total_deposits)}</td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-white">{formatCurrency(row.total_credit)}</td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-yellow-400">{(row.total_deposits / row.total_credit).toFixed(2)}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    </div>
);