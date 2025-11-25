// frontend/src/components/LineChart.jsx
import React from 'react'
import { Line } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend } from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend)

export default function LineChart({ title, labels = [], series = [], label = 'Deposits' }) {
  const data = {
    labels,
    datasets: [
      {
        label,
        data: series,
        tension: 0.3,
        borderColor: 'rgba(99,102,241,1)',
        backgroundColor: 'rgba(99,102,241,0.2)',
        fill: true,
      }
    ]
  }

  const options = {
    responsive: true,
    plugins: {
      title: { display: !!title, text: title, color: '#e5e7eb' },
      legend: { labels: { color: '#d1d5db' } }
    },
    scales: {
      x: { ticks: { color: '#cbd5e1' }, grid: { color: 'rgba(255,255,255,0.03)' } },
      y: { ticks: { color: '#cbd5e1' }, grid: { color: 'rgba(255,255,255,0.03)' } }
    }
  }

  return <div className="bg-gray-800 p-4 rounded h-64"><Line options={options} data={data} /></div>
}
