// frontend/src/ui/Charts.jsx
import React from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import { Line, Doughnut, Bar, Scatter } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Tooltip,
  Legend,
  Filler
);

export const PALETTE = {
  primary: "#4f46e5",
  accent: "#06b6d4",
  warm: "#f59e0b",
  danger: "#ef4444",
  neutral: "#64748b",
  bg: "#f4f7fb",
  card: "#ffffff",
  muted: "#9aa6b3",
};

export const COMPONENT_COLORS = {
  totalaggregatedeposits: "#4f46e5",
  totalcurrentdeposits: "#7c3aed",
  totalsavingsdeposits: "#06b6d4",
  totaltermdeposits: "#f59e0b",
  totalaggregatecredit: "#0ea5a0",
  totalretailcredit: "#06b6d4",
  totalagriculturecredit: "#a78bfa",
  totalbusinesscredit: "#fb923c",
  totaltransactionscount: "#ef4444",
};

function baseOptions({ stacked = false } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { boxWidth: 12, boxHeight: 6, padding: 12, usePointStyle: true } },
      tooltip: { mode: "index", intersect: false },
    },
    scales: { x: { grid: { display: false } }, y: { grid: { color: "rgba(15,30,60,0.06)" }, beginAtZero: true } },
    interaction: { intersect: false, mode: "nearest" },
    stacked,
  };
}

function styleDatasets(datasets, fill = true) {
  return (datasets || []).map((ds, i) => {
    const label = ds.label;
    const color = COMPONENT_COLORS[label] || "#94a3b8";
    const bg = fill ? (color + "33") : color;
    return {
      ...ds,
      borderColor: color,
      backgroundColor: bg,
      borderWidth: 2,
      pointRadius: 2,
      tension: 0.3,
    };
  });
}

export function StackedAreaChart({ data }) {
  if (!data || !data.labels || !data.datasets) return <div style={{ color: PALETTE.muted, padding: 20 }}>No time-series data available</div>;
  if (data.labels.length < 2) return <div style={{ color: PALETTE.muted, padding: 20 }}>Not enough data points to show a trend (need 2+ periods).</div>;
  const cfg = { data: { labels: data.labels, datasets: styleDatasets(data.datasets, true) }, options: baseOptions({ stacked: true }) };
  return <div style={{ height: 320 }}><Line {...cfg} /></div>;
}

export function DonutChart({ data }) {
  if (!data || !data.datasets || !data.labels) return <div style={{ color: PALETTE.muted }}>No data</div>;
  return <div style={{ height: 200 }}><Doughnut data={data} options={{ maintainAspectRatio: false, plugins: { legend: { position: "bottom" } } }} /></div>;
}

export function SimpleBar({ data }) {
  if (!data || !data.labels || !data.datasets) return <div style={{ color: PALETTE.muted }}>No data</div>;
  if (data.labels.length < 1) return <div style={{ color: PALETTE.muted, padding: 20 }}>No bar data</div>;
  const styled = (data.datasets || []).map(ds => ({ ...ds, backgroundColor: COMPONENT_COLORS[ds.label] || "#94a3b8" }));
  const cfg = { data: { labels: data.labels, datasets: styled }, options: baseOptions({ stacked: false }) };
  return <div style={{ height: 220 }}><Bar {...cfg} /></div>;
}

export function ScatterChart({ data }) {
  if (!data || !data.datasets) return <div style={{ color: PALETTE.muted }}>No data</div>;
  return <div style={{ height: 220 }}><Scatter data={data} options={{ ...baseOptions(), scales: { x: { title: { display: true, text: "Transactions" } }, y: { title: { display: true, text: "Deposits" } } } }} /></div>;
}

export function Spark({ data }) {
  if (!data || !data.labels || !data.datasets) return <div style={{ color: PALETTE.muted }}>—</div>;
  const cfg = { data: { labels: data.labels, datasets: [{ data: data.datasets[0].data, borderColor: PALETTE.primary, borderWidth: 2, pointRadius: 0, fill: false }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { display: false } } } };
  return <div style={{ height: 36, width: 120 }}><Line {...cfg} /></div>;
}
