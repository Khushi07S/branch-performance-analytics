// src/ui/KpiCards.jsx
import React from "react";

function Kpi({ title, value, meta, accent }) {
  return (
    <div className="kpi-card">
      <h3>{title}</h3>
      <div className="value" style={{ color: accent || "#0f172a" }}>{value}</div>
      <div className="meta">{meta}</div>
    </div>
  );
}

export default function KpiCards({ kpiData }) {
  // kpiData is expected to be an object with numeric values
  if (!kpiData) {
    // placeholder skeleton
    return (
      <div className="kpi-grid">
        <Kpi title="Total Deposits" value="—" meta="No data" />
        <Kpi title="Total Credit" value="—" meta="No data" />
        <Kpi title="CASA" value="—" meta="No data" />
        <Kpi title="Active Branches" value="—" meta="No data" />
      </div>
    );
  }

  return (
    <div className="kpi-grid">
      <Kpi title="Total Deposits (National)" value={format(kpiData.total_deposits)} meta="Aggregate last 3 quarters" accent="#0ea5a4" />
      <Kpi title="Total Credit (National)" value={format(kpiData.total_credit)} meta="Aggregate last 3 quarters" accent="#4f46e5" />
      <Kpi title="Total CASA" value={format(kpiData.total_casa)} meta="Current & Savings Account" accent="#f59e0b" />
      <Kpi title="Active Branches" value={kpiData.total_branches ?? "—"} meta="Across all regions" accent="#8b5cf6" />
    </div>
  );
}

function format(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number" && v >= 1000) {
    return `₹${(v/10000000).toFixed(2)} Cr`;
  }
  return `₹${v}`;
}
