// src/ui/RegionalTable.jsx
import React from "react";

export default function RegionalTable({ data = [] }) {
  return (
    <div className="panel">
      <h3 style={{marginTop:0}}>Branch Performance Overview</h3>
      <table className="regional-table" aria-hidden={data.length===0}>
        <thead>
          <tr>
            <th style={{width:'40%'}}>Branch</th>
            <th>Deposits</th>
            <th>Credit</th>
            <th className="small">Ratio D/C</th>
          </tr>
        </thead>
        <tbody>
          {data.length === 0 && <tr><td colSpan={4} className="small">No regional data</td></tr>}
          {data.slice(0,6).map((r, i) => (
            <tr key={i}>
              <td style={{fontWeight:700}}>{r.branches}</td>
              <td>{fmt(r.totalaggregatedeposits)}</td>
              <td>{fmt(r.totalaggregatecredit)}</td>
              <td className="small">{safeRatio(r.totalaggregatedeposits, r.totalaggregatecredit)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function fmt(v){ if (v==null) return "—"; return `₹${(v/1000).toFixed(1)}K` }
function safeRatio(a,b){ if (!a || !b) return "—"; return (a/b).toFixed(2) }
