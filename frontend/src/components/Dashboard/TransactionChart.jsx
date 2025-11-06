import React from 'react';
import Plot from 'react-plotly.js';

const TransactionChart = ({ data, title = "Monthly Transaction Volume" }) => {
  // Default sample data if no data provided
  const defaultData = {
    months: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    deposits: [450, 520, 480, 590, 620, 580, 650, 700, 680, 720, 750, 800],
    credits: [320, 380, 350, 420, 450, 430, 480, 520, 500, 540, 580, 620],
  };

  const chartData = data || defaultData;

  const traces = [
    {
      x: chartData.months,
      y: chartData.deposits,
      name: 'Deposits',
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: '#3b82f6', width: 3 },
      marker: { size: 8, color: '#3b82f6' },
    },
    {
      x: chartData.months,
      y: chartData.credits,
      name: 'Credits',
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: '#10b981', width: 3 },
      marker: { size: 8, color: '#10b981' },
    },
  ];

  const layout = {
    title: {
      text: title,
      font: { size: 16, weight: 600, color: '#111827' },
    },
    xaxis: {
      title: 'Month',
      gridcolor: '#f3f4f6',
    },
    yaxis: {
      title: 'Volume (in Millions)',
      gridcolor: '#f3f4f6',
    },
    plot_bgcolor: '#ffffff',
    paper_bgcolor: '#ffffff',
    margin: { l: 60, r: 40, t: 60, b: 60 },
    hovermode: 'x unified',
    legend: {
      orientation: 'h',
      x: 0.5,
      xanchor: 'center',
      y: -0.15,
    },
  };

  const config = {
    responsive: true,
    displayModeBar: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-6">
      <Plot
        data={traces}
        layout={layout}
        config={config}
        style={{ width: '100%', height: '400px' }}
        useResizeHandler={true}
      />
    </div>
  );
};

export default TransactionChart;