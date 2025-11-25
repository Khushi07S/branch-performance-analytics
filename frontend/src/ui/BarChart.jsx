// frontend/src/components/BarChart.jsx
import React from 'react'
import { Bar } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend } from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend)

export default function BarChart({ title, labels = [], datasetA = [], datasetB = [], labelA='Deposits', labelB='Credit' }) {
  const data = {
    labels,
    datasets: [
      {
        label: labelA,
        data: datasetA,
        backgroundColor: 'rgba(99,102,241,0.8)',
      },
      {
        label: labelB,
        data: datasetB,
        backgroundColor: 'rgba(20,184,166,0.8)',
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

  return <div className="bg-gray-800 p-4 rounded h-64"><Bar options={options} data={data} /></div>
}
