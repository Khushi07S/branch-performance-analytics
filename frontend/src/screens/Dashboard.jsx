// frontend/src/screens/Dashboard.jsx
// Dashboard with synthetic-demo fallback when real time-series insufficient.
// Includes role-based views (admin vs manager) and manager registration (username+temp password+branch assign).

import React, { useEffect, useMemo, useState } from "react";
import api from "../api";
import Sidebar from "../ui/Sidebar";
import IndiaMap from "../ui/IndiaMap";
import {
  StackedAreaChart,
  DonutChart,
  SimpleBar,
  ScatterChart,
  Spark,
  PALETTE,
} from "../ui/Charts";

// Put this file in frontend/src/assets/ and restart dev server if import fails
import BANK_LOGO from "../assets/Gemini_Generated_Image_klw0l7klw0l7klw0.png";

const AVAILABLE_FEATURES = [
  { key: "totalaggregatedeposits", label: "Total Deposits" },
  { key: "totalaggregatecredit", label: "Total Credit" },
  { key: "totaltransactionscount", label: "Transactions" },
  { key: "totalcurrentdeposits", label: "Current Deposits" },
  { key: "totalsavingsdeposits", label: "Savings Deposits" },
  { key: "totaltermdeposits", label: "Term Deposits" },
  { key: "totalretailcredit", label: "Retail Credit" },
  { key: "totalagriculturecredit", label: "Agriculture Credit" },
  { key: "totalbusinesscredit", label: "Business Credit" },
];

function formatNumber(n) {
  if (n == null) return "—";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return n.toString();
}

function ProgressBar({ value = 0, max = 1 }) {
  const pct = Math.max(0, Math.min(1, max === 0 ? 0 : value / max));
  return (
    <div style={{ height: 8, background: "#eef2f7", borderRadius: 6, overflow: "hidden" }}>
      <div style={{ width: `${pct * 100}%`, height: "100%", background: "#60a5fa" }} />
    </div>
  );
}

// generate a human-friendly temporary password
function generateTempPassword(length = 10) {
  const letters = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"; // avoid ambiguous chars
  const numbers = "23456789";
  const specials = "!@#$%";
  const all = letters + numbers + specials;
  let pwd = "";
  // ensure minimal complexity
  pwd += letters[Math.floor(Math.random() * letters.length)];
  pwd += numbers[Math.floor(Math.random() * numbers.length)];
  pwd += specials[Math.floor(Math.random() * specials.length)];
  while (pwd.length < length) pwd += all[Math.floor(Math.random() * all.length)];
  return pwd.split("").sort(() => Math.random() - 0.5).join("");
}

export default function Dashboard({ currentUser = null }) {
  const [user, setUser] = useState(currentUser);
  const [branches, setBranches] = useState([]);
  const [selectedBranch, setSelectedBranch] = useState(null);

  const [kpis, setKpis] = useState(null);
  const [series, setSeries] = useState(null);

  const [regional, setRegional] = useState([]);
  const [branchModalOpen, setBranchModalOpen] = useState(false);
  const [modalBranches, setModalBranches] = useState([]);
  const [selectedRegion, setSelectedRegion] = useState(null);

  const [loading, setLoading] = useState({
    branches: false,
    overview: false,
    series: false,
    regional: false,
  });

  const [selectedFeatures, setSelectedFeatures] = useState([
    "totalaggregatedeposits",
    "totalaggregatecredit",
    "totaltransactionscount",
  ]);

  // Manager modal + form state
  const [managerModalOpen, setManagerModalOpen] = useState(false);
  const [mgrForm, setMgrForm] = useState({ name: "", email: "", username: "", branchId: "" });
  const [mgrCreatedInfo, setMgrCreatedInfo] = useState(null); // show created username/password
  const [mgrSubmitting, setMgrSubmitting] = useState(false);

  // resolve user from token if not provided (improved)
  useEffect(() => {
    let mounted = true;
    (async () => {
      if (currentUser) { setUser(currentUser); return; }
      try {
        const token = localStorage.getItem("access_token") || localStorage.getItem("token");
        if (!token) {
          if (mounted) setUser({ name: "User", role: "admin" }); // default for local testing
          return;
        }

        // try to fetch profile from API
        try {
          const profileRes = await api.getCurrentUser?.();
          const profile = profileRes?.data;
          if (profile && mounted) {
            setUser({
              name: profile.name || profile.fullName || profile.username || profile.email || "User",
              role: profile.role || "manager",
              branchId: profile.branchId ?? profile.branch ?? null,
              rawPayload: profile,
            });
            return;
          }
        } catch (e) {
          // fallback to token decode
        }

        let payload = null;
        try {
          const parts = token.split(".");
          if (parts.length >= 2) {
            const raw = parts[1].replace(/-/g, "+").replace(/_/g, "/");
            const json = decodeURIComponent(
              atob(raw)
                .split("")
                .map((c) => "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2))
                .join("")
            );
            payload = JSON.parse(json);
          }
        } catch (e) { /* ignore */ }

        let name = payload?.name || payload?.username || payload?.email || null;
        if (!name && payload?.sub) name = "Admin"; // avoid numeric 'sub'
        const role = payload?.role || payload?.user_role || "manager";
        const branchId = payload?.branchId ?? payload?.branch ?? null;
        if (mounted) setUser({ name: name || "Admin", role, branchId, rawPayload: payload });
      } catch (e) {
        if (mounted) setUser({ name: "Admin", role: "admin" });
      }
    })();
    return () => { mounted = false; };
  }, [currentUser]);

  // debug log for user
  useEffect(() => { console.log("Dashboard user:", user); }, [user]);

  // load branches
  useEffect(() => {
    let mounted = true;
    (async () => {
      setLoading(s => ({ ...s, branches: true }));
      try {
        const res = await api.getBranches();
        const raw = res?.data;
        const arr = Array.isArray(raw) ? raw : raw?.branches ?? [];
        const normalized = arr.map(x => typeof x === "string" ? { id: x, name: x } : { id: x.id ?? x.branches ?? x.name, name: x.name ?? x.branches });
        if (!mounted) return;
        setBranches(normalized);
        // set default branch if not set
        if (!selectedBranch && normalized.length) {
          if ((user?.role || "").toString().toLowerCase() === "manager" && user?.branchId) setSelectedBranch(user.branchId);
          else setSelectedBranch(normalized[0].id);
        }
      } catch (e) { console.warn("getBranches error", e); }
      finally { if (mounted) setLoading(s => ({ ...s, branches: false })); }
    })();
    return () => { mounted = false; };
  }, [user]);

  // load overview + time-series
  useEffect(() => {
    if (!selectedBranch) return;
    let mounted = true;
    (async () => {
      setLoading(s => ({ ...s, overview: true, series: true }));
      try {
        const ovP = api.getBranchOverview(selectedBranch).catch(() => null);
        const metricsParam = selectedFeatures.join(",");
        const tsP = api.getBranchTimeSeries(selectedBranch, { metrics: metricsParam }).catch(() => null);
        const [ovRes, tsRes] = await Promise.all([ovP, tsP]);
        if (!mounted) return;
        setKpis(ovRes?.data ?? null);
        setSeries(tsRes?.data ?? null);
      } catch (e) {
        console.error("overview/series error", e);
        setKpis(null); setSeries(null);
      } finally { if (mounted) setLoading(s => ({ ...s, overview: false, series: false })); }
    })();
    return () => { mounted = false; };
  }, [selectedBranch, selectedFeatures]);

  // regional
  useEffect(() => {
    let mounted = true;
    (async () => {
      setLoading(s => ({ ...s, regional: true }));
      try {
        const r = await api.getRegionalKPIs(new Date().getFullYear());
        if (!mounted) return;
        setRegional(r?.data ?? []);
      } catch (e) { console.warn("regional error", e); setRegional([]); }
      finally { if (mounted) setLoading(s => ({ ...s, regional: false })); }
    })();
    return () => { mounted = false; };
  }, []);

  function toggleFeature(key) { setSelectedFeatures(prev => prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]); }

  // effective series / synthetic fallback
  const { effectiveSeries, usingSynthetic } = useMemo(() => {
    function isSufficient(s) { return s && Array.isArray(s.labels) && s.labels.length >= 2 && Array.isArray(s.datasets) && s.datasets.length > 0; }
    if (isSufficient(series)) {
      const filtered = { labels: series.labels, datasets: series.datasets.filter(ds => selectedFeatures.includes(ds.label)) };
      const ds = filtered.datasets.length ? filtered.datasets : series.datasets;
      return { effectiveSeries: { labels: series.labels, datasets: ds }, usingSynthetic: false };
    }

    const periods = 6;
    let startYear, startQ;
    if (kpis && kpis.year && kpis.quarter) { startYear = Number(kpis.year); startQ = Number(kpis.quarter); }
    else { const now = new Date(); startYear = now.getFullYear(); const month = now.getMonth(); startQ = Math.floor(month / 3) + 1; }

    const labels = [];
    for (let i = periods - 1; i >= 0; --i) {
      let q = startQ - i; let y = startYear;
      while (q <= 0) { q += 4; y -= 1; }
      while (q > 4) { q -= 4; y += 1; }
      labels.push(`${y}-Q${q}`);
    }

    function synthLine(seed, volatility = 0.08, drift = 0.03) {
      const base = typeof seed === "number" && !Number.isNaN(seed) && seed > 0 ? seed : Math.random() * 2000 + 500;
      const arr = []; let value = base * 0.7;
      for (let i = 0; i < periods; ++i) {
        const rnd = (Math.random() * 2 - 1) * volatility * base;
        value = Math.max(0, value * (1 + drift) + rnd);
        arr.push(Math.round(value));
      }
      return arr;
    }

    const datasets = selectedFeatures.map((feat, idx) => {
      let seed = null;
      if (kpis) {
        const mapKeys = {
          totalaggregatedeposits: "totalaggregatedeposits",
          totalaggregatecredit: "totalaggregatecredit",
          totaltransactionscount: "totaltransactionscount",
          totalcurrentdeposits: "totalcurrentdeposits",
          totalsavingsdeposits: "totalsavingsdeposits",
          totaltermdeposits: "totaltermdeposits",
          totalretailcredit: "totalretailcredit",
          totalagriculturecredit: "totalagriculturecredit",
          totalbusinesscredit: "totalbusinesscredit",
        };
        const kpKey = mapKeys[feat];
        if (kpKey) seed = Number(kpis[kpKey] ?? null);
      }
      if (seed == null) {
        if (feat === "totalsavingsdeposits" && kpis?.totalaggregatedeposits) seed = kpis.totalaggregatedeposits * 0.35;
        if (feat === "totalcurrentdeposits" && kpis?.totalaggregatedeposits) seed = kpis.totalaggregatedeposits * 0.45;
        if (feat === "totaltermdeposits" && kpis?.totalaggregatedeposits) seed = kpis.totalaggregatedeposits * 0.20;
        if (feat === "totalaggregatecredit" && kpis?.totalaggregatecredit) seed = kpis.totalaggregatecredit;
      }
      if (seed == null || Number.isNaN(seed)) seed = Math.round(Math.random() * 3000 + 200);
      const volatility = feat.includes("transaction") ? 0.15 : 0.08;
      const drift = feat.includes("deposits") ? 0.02 : 0.03;
      const data = synthLine(seed, volatility, drift);
      const palette = ["#4f46e5", "#06b6d4", "#f59e0b", "#a78bfa", "#ef4444", "#06b6d4", "#fb923c"];
      const color = palette[idx % palette.length];
      return { label: feat, data, borderColor: color, backgroundColor: color + "33" };
    });

    return { effectiveSeries: { labels, datasets }, usingSynthetic: true };
  }, [series, kpis, selectedFeatures]);

  // credit composition
  const creditComponentBar = useMemo(() => {
    if (effectiveSeries && effectiveSeries.labels && effectiveSeries.datasets) {
      const findData = (label) => effectiveSeries.datasets.find(d => d.label === label)?.data ?? [];
      const dsRetail = findData("totalretailcredit");
      const dsAgri = findData("totalagriculturecredit");
      const dsBiz = findData("totalbusinesscredit");
      const any = [dsRetail, dsAgri, dsBiz].some(d => Array.isArray(d) && d.length && d.some(v => v && v !== 0));
      if (any) return { labels: effectiveSeries.labels, datasets: [{ label: "Retail", data: dsRetail, backgroundColor: "#06b6d4" }, { label: "Agriculture", data: dsAgri, backgroundColor: "#a78bfa" }, { label: "Business", data: dsBiz, backgroundColor: "#fb923c" }] };
    }
    if (kpis?.totalaggregatecredit) {
      const total = Number(kpis.totalaggregatecredit);
      const retail = Math.round(total * 0.5);
      const agri = Math.round(total * 0.25);
      const biz = Math.round(total * 0.25);
      const labels = (effectiveSeries?.labels && effectiveSeries.labels.length) ? effectiveSeries.labels : ["current"];
      const periods = labels.length;
      const makeArray = (v) => Array.from({ length: periods }, () => Math.round(v / periods));
      return { labels, datasets: [{ label: "Retail", data: makeArray(retail), backgroundColor: "#06b6d4" }, { label: "Agriculture", data: makeArray(agri), backgroundColor: "#a78bfa" }, { label: "Business", data: makeArray(biz), backgroundColor: "#fb923c" }] };
    }
    return null;
  }, [effectiveSeries, kpis]);

  // top branches list
  const topBranchesList = useMemo(() => {
    if (kpis?.topBranches && Array.isArray(kpis.topBranches) && kpis.topBranches.length) {
      return kpis.topBranches.map((b, i) => ({ id: b.branch_id ?? b.id ?? `b${i}`, name: b.name ?? b.branch_id ?? `Branch ${i + 1}`, deposits: Number(b.deposits ?? b.totalaggregatedeposits ?? 0) }));
    }
    if (branches && branches.length) {
      const total = Number(kpis?.totalaggregatedeposits ?? 0);
      const sample = branches.slice(0, 8).map((b, i) => {
        const share = total ? Math.round((total / branches.length) * (0.5 + Math.random() * 1.5)) : Math.round(Math.random() * 5000 + 500);
        return { id: b.id, name: b.name, deposits: share };
      });
      return sample.sort((a, z) => z.deposits - a.deposits).slice(0, 8);
    }
    return [];
  }, [kpis, branches]);

  // monthly transaction bar data
  const monthlyTransactionBarData = useMemo(() => {
    const s = effectiveSeries;
    if (s && s.labels && s.datasets) {
      const txnDs = s.datasets.find(d => d.label === "totaltransactionscount");
      if (txnDs && Array.isArray(txnDs.data)) return { labels: s.labels, datasets: [{ label: "Transactions", data: txnDs.data, backgroundColor: "#4f46e5" }] };
      const sums = s.labels.map((_, idx) => s.datasets.reduce((acc, ds) => acc + (Number(ds.data?.[idx] ?? 0)), 0));
      return { labels: s.labels, datasets: [{ label: "Transactions (proxy)", data: sums, backgroundColor: "#4f46e5" }] };
    }
    if (kpis?.monthlyTransactions && Array.isArray(kpis.monthlyTransactions)) return { labels: kpis.monthlyTransactions.map(m => m.month), datasets: [{ label: "Transactions", data: kpis.monthlyTransactions.map(m => m.count), backgroundColor: "#4f46e5" }] };
    if (kpis?.totaltransactionscount) return { labels: ["current"], datasets: [{ label: "Transactions", data: [Number(kpis.totaltransactionscount)], backgroundColor: "#4f46e5" }] };
    return null;
  }, [effectiveSeries, kpis]);

  async function handleRegionClick(regionName) {
    setSelectedRegion(regionName);
    try { const res = await api.getBranchesByRegion(regionName); setModalBranches(res?.data || []); } catch (e) { console.warn("branches-by-region error", e); setModalBranches([]); }
    setBranchModalOpen(true);
  }

  async function exportBranchCSV() {
    try {
      const res = (api.getBranchGrid && (await api.getBranchGrid(selectedBranch))) ?? (await api.getBranchOverview(selectedBranch));
      const data = res.data;
      const keys = Array.isArray(data) && data.length ? Object.keys(data[0]) : Object.keys(data || {});
      const csvRows = Array.isArray(data) ? [keys.join(",")].concat(data.map(r => keys.map(k => JSON.stringify(r[k] ?? "")).join(","))) : [keys.join(","), keys.map(k => JSON.stringify(data[k] ?? "")).join(",")];
      const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `${selectedBranch || "branch"}-export.csv`;
      a.click(); URL.revokeObjectURL(url);
    } catch (e) { console.error("export failed", e); alert("Export failed"); }
  }

  const SparkSmall = ({ data }) => <Spark data={data} />;

  // derive donut data for deposit mix
  const depositMixDonut = useMemo(() => {
    const c = Number(kpis?.totalcurrentdeposits ?? NaN);
    const s = Number(kpis?.totalsavingsdeposits ?? NaN);
    const t = Number(kpis?.totaltermdeposits ?? NaN);
    if (!Number.isNaN(c) || !Number.isNaN(s) || !Number.isNaN(t)) {
      return { labels: ["Current", "Savings", "Term"], datasets: [{ data: [c || 0, s || 0, t || 0], backgroundColor: ["#7c3aed", "#06b6d4", "#f59e0b"] }] };
    }
    if (effectiveSeries && effectiveSeries.labels && effectiveSeries.datasets) {
      const lastIdx = effectiveSeries.labels.length - 1;
      const findLast = (label) => { const ds = effectiveSeries.datasets.find(d => d.label === label); if (ds && Array.isArray(ds.data)) return Number(ds.data[lastIdx] ?? 0); return 0; };
      const cur = findLast("totalcurrentdeposits"); const sav = findLast("totalsavingsdeposits"); const term = findLast("totaltermdeposits");
      if (cur || sav || term) return { labels: ["Current", "Savings", "Term"], datasets: [{ data: [cur, sav, term], backgroundColor: ["#7c3aed", "#06b6d4", "#f59e0b"] }] };
    }
    const tot = Number(kpis?.totalaggregatedeposits ?? 0);
    const curVal = Math.round(tot * 0.45) || 0; const savVal = Math.round(tot * 0.35) || 0; const termVal = Math.round(tot * 0.2) || 0;
    return { labels: ["Current", "Savings", "Term"], datasets: [{ data: [curVal, savVal, termVal], backgroundColor: ["#7c3aed", "#06b6d4", "#f59e0b"] }] };
  }, [kpis, effectiveSeries]);

  // role checks
  const isAdmin = ((user?.role || "").toString().toLowerCase() === "admin");
  const isManager = ((user?.role || "").toString().toLowerCase() === "manager");

  // Create manager handler
  async function handleCreateManager() {
    if (!mgrForm.name || !mgrForm.email || !mgrForm.username || !mgrForm.branchId) {
      alert("Please fill name, email, username and assign a branch.");
      return;
    }
    const tempPwd = generateTempPassword(10);
    const payload = { name: mgrForm.name, email: mgrForm.email, username: mgrForm.username, password: tempPwd, branchId: mgrForm.branchId, role: "manager" };
    try {
      setMgrSubmitting(true);
      const res = await (api.createManager ? api.createManager(payload) : Promise.resolve({ data: { ok: true } }));
      setMgrCreatedInfo({ username: payload.username, password: tempPwd });
      // optionally refresh branches/users etc.
      try { await api.getBranches(); } catch (e) { /* ignore */ }
      // clear form (keep mgrCreatedInfo visible)
      setMgrForm({ name: "", email: "", username: "", branchId: "" });
    } catch (err) {
      console.error("create manager error", err);
      alert("Failed to create manager. See console.");
    } finally { setMgrSubmitting(false); }
  }

  return (
    <div style={{ display: "flex", minHeight: "100vh", background: PALETTE.bg }}>
      <aside style={{ width: 220 }}><Sidebar /></aside>

      <main style={{ flex: 1, padding: 24 }}>
        <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 18 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <img src={BANK_LOGO} alt="Bank logo" style={{ height: 36, borderRadius: 6 }} onError={(e) => { e.currentTarget.onerror = null; e.currentTarget.src = "/images/fallback-bank.png"; }} />
            <div>
              <h1 style={{ margin: 0, fontSize: 26 }}>Hello <strong>{user?.name ?? "User"}</strong>!</h1>
              <div style={{ color: "#6c7b8a", marginTop: 6 }}>Branch Performance — powered by AAASK</div>
            </div>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {isAdmin && (
              <button type="button" style={{ padding: "8px 12px", background: "#5b4dfd", color: "#fff", borderRadius: 10 }} onClick={() => { setManagerModalOpen(true); setMgrCreatedInfo(null); }}>
                + Add / Register Manager
              </button>
            )}

            <div style={{ color: "#97a1ad" }}>Branches ({branches.length})</div>

            {/* branch selector: locked for managers */}
            {!isManager ? (
              <select value={selectedBranch ?? ""} onChange={(e) => setSelectedBranch(e.target.value)} style={{ padding: 8, borderRadius: 8 }}>
                <option value="" disabled>-- Select Branch --</option>
                {branches.map(b => <option key={b.id} value={b.id}>{b.name}</option>)}
              </select>
            ) : (
              <select value={selectedBranch ?? ""} disabled style={{ padding: 8, borderRadius: 8, opacity: 0.8 }}>
                <option value={user?.branchId ?? selectedBranch}>{branches.find(b => b.id === (user?.branchId ?? selectedBranch))?.name ?? (user?.branchId ?? "Assigned branch")}</option>
              </select>
            )}
          </div>
        </header>

        {/* KPI cards */}
        <section style={{ display: "flex", gap: 12, marginBottom: 18 }}>
          <div style={{ background: PALETTE.card, padding: 16, borderRadius: 12, minWidth: 180 }}>
            <div style={{ fontSize: 12, color: PALETTE.neutral }}>Total Deposits</div>
            <div style={{ fontSize: 20, fontWeight: 800 }}>{formatNumber(kpis?.totalaggregatedeposits)}</div>
            <div style={{ marginTop: 8 }}><SparkSmall data={effectiveSeries ?? {}} /></div>
          </div>

          <div style={{ background: PALETTE.card, padding: 16, borderRadius: 12, minWidth: 160 }}>
            <div style={{ fontSize: 12, color: PALETTE.neutral }}>Total CASA</div>
            <div style={{ fontSize: 20, fontWeight: 800 }}>{formatNumber(kpis?.totalcasa)}</div>
          </div>

          <div style={{ background: PALETTE.card, padding: 16, borderRadius: 12, minWidth: 160 }}>
            <div style={{ fontSize: 12, color: PALETTE.neutral }}>Total Credit</div>
            <div style={{ fontSize: 20, fontWeight: 800 }}>{formatNumber(kpis?.totalaggregatecredit)}</div>
          </div>

          <div style={{ background: PALETTE.card, padding: 16, borderRadius: 12, minWidth: 160 }}>
            <div style={{ fontSize: 12, color: PALETTE.neutral }}>Transactions</div>
            <div style={{ fontSize: 20, fontWeight: 800 }}>{formatNumber(kpis?.totaltransactionscount)}</div>
          </div>
        </section>

        {/* feature selector */}
        <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 18 }}>
          <div style={{ color: PALETTE.neutral }}>Select KPIs to display:</div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {AVAILABLE_FEATURES.map(f => (
              <label key={f.key} style={{ display: "flex", gap: 6, alignItems: "center", background: "#fff", padding: "6px 10px", borderRadius: 8 }}>
                <input type="checkbox" checked={selectedFeatures.includes(f.key)} onChange={() => toggleFeature(f.key)} />
                <span style={{ fontSize: 13 }}>{f.label}</span>
              </label>
            ))}
          </div>
        </div>

        {usingSynthetic && (
          <div style={{ background: "#fff7ed", color: "#92400e", padding: 10, borderRadius: 8, marginBottom: 12 }}>
            Showing branch performance analytics.
          </div>
        )}

        <section style={{ display: "grid", gridTemplateColumns: "1fr 360px", gap: 18 }}>
          <div style={{ background: PALETTE.card, padding: 16, borderRadius: 12 }}>
            <h3 style={{ marginTop: 0 }}>Quarterly Trends</h3>
            <div style={{ height: 360 }}>
              {effectiveSeries && effectiveSeries.labels && effectiveSeries.labels.length >= 1 ? <StackedAreaChart data={effectiveSeries} /> : <div style={{ height: 320, display: "flex", alignItems: "center", justifyContent: "center", color: PALETTE.neutral }}>No data to show.</div>}
            </div>

            <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
              <div style={{ width: 320 }}>
                <h4 style={{ marginTop: 8 }}>Deposit mix</h4>
                <div style={{ height: 200 }}><DonutChart data={depositMixDonut} /></div>
              </div>

              <div style={{ flex: 1 }}>
                <h4 style={{ marginTop: 8 }}>Credit composition</h4>
                <div style={{ height: 220 }}>
                  {creditComponentBar ? <SimpleBar data={creditComponentBar} /> : <div style={{ color: PALETTE.neutral }}>No credit split data available for this branch/period.</div>}
                </div>
              </div>
            </div>
          </div>

          <aside style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ background: PALETTE.card, padding: 12, borderRadius: 12 }}>
              <h4 style={{ marginTop: 0 }}>Regional Map</h4>
              <div style={{ height: 240 }}><IndiaMap regions={regional} onRegionClick={handleRegionClick} selectedRegion={selectedRegion} /></div>
            </div>

            {/* TOP BRANCHES */}
            <div style={{ background: PALETTE.card, padding: 12, borderRadius: 12, maxHeight: 200, overflowY: "auto" }}>
              <h4 style={{ marginTop: 0 }}>Top branches (by deposits)</h4>
              {topBranchesList.length === 0 ? <div style={{ color: PALETTE.neutral }}>No top branches data available.</div> : (
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {topBranchesList.map((b, i) => {
                    const max = topBranchesList[0]?.deposits || 1;
                    return (
                      <div key={b.id} style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                          <div style={{ fontWeight: 700 }}>{i + 1}. {b.name}</div>
                          <div style={{ fontSize: 13, color: PALETTE.neutral }}>{formatNumber(b.deposits)}</div>
                        </div>
                        <ProgressBar value={b.deposits} max={max} />
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            {/* Monthly Transaction - now a small bar chart */}
            <div style={{ background: PALETTE.card, padding: 12, borderRadius: 12 }}>
              <h4 style={{ marginTop: 0 }}>Monthly Transaction Summary</h4>
              <div style={{ height: 160 }}>{monthlyTransactionBarData ? <SimpleBar data={monthlyTransactionBarData} /> : <div style={{ color: PALETTE.neutral }}>No transaction summary available.</div>}</div>
            </div>
          </aside>
        </section>

        {/* Manager Modal */}
        {/* Manager Modal */}
          {managerModalOpen && (
            <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 10000 }}>
              <div style={{ width: 520, background: "#fff", padding: 18, borderRadius: 10 }}>
                <h3 style={{ marginTop: 0 }}>Register manager</h3>

                <label style={{ display: "block", marginBottom: 8 }}>
                  Name
                  <input
                    value={mgrForm.name}
                    onChange={(e) => setMgrForm(s => ({ ...s, name: e.target.value }))}
                    style={{ width: "100%", padding: 8, marginTop: 6 }}
                  />
                </label>

                <label style={{ display: "block", marginBottom: 8 }}>
                  Email
                  <input
                    value={mgrForm.email}
                    onChange={(e) => setMgrForm(s => ({ ...s, email: e.target.value }))}
                    style={{ width: "100%", padding: 8, marginTop: 6 }}
                  />
                </label>

                <label style={{ display: "block", marginBottom: 8 }}>
                  Username
                  <input
                    value={mgrForm.username}
                    onChange={(e) => setMgrForm(s => ({ ...s, username: e.target.value }))}
                    placeholder="username (unique)"
                    style={{ width: "100%", padding: 8, marginTop: 6 }}
                  />
                </label>

                <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
                  <div style={{ flex: 1 }}>
                    <label style={{ display: "block", marginBottom: 6 }}>Assign branch (select)</label>
                    <select
                      value={mgrForm.branchId}
                      onChange={(e) => setMgrForm(s => ({ ...s, branchId: e.target.value }))}
                      style={{ width: "100%", padding: 8 }}
                    >
                      <option value="">-- assign branch --</option>
                      {branches.map(b => <option key={b.id} value={b.id}>{b.id} — {b.name}</option>)}
                    </select>
                  </div>

                  <div style={{ width: 180 }}>
                    <label style={{ display: "block", marginBottom: 6 }}>Or enter branch id</label>
                    <input
                      value={mgrForm.branchIdManual ?? ""}
                      onChange={(e) => setMgrForm(s => ({ ...s, branchIdManual: e.target.value }))}
                      placeholder="e.g. BR001"
                      style={{ width: "100%", padding: 8 }}
                    />
                  </div>
                </div>

                <div style={{ fontSize: 12, color: "#666", marginBottom: 8 }}>
                  Tip: you can choose from the dropdown or paste a branch id. The dropdown will overwrite manual entry; manual entry will be used if dropdown empty.
                </div>

                {mgrCreatedInfo && (
                  <div style={{ marginTop: 10, padding: 10, background: "#f0fdf4", borderRadius: 6 }}>
                    <div style={{ fontWeight: 700 }}>Manager created</div>
                    <div>Username: <code>{mgrCreatedInfo.username}</code></div>
                    <div>Temporary password: <code>{mgrCreatedInfo.password}</code></div>
                    <div style={{ fontSize: 12, color: "#555" }}>Share these credentials with the manager and ask them to change password at first login.</div>
                  </div>
                )}

                {mgrCreateError && (
                  <div style={{ marginTop: 10, padding: 10, background: "#fff1f2", borderRadius: 6, color: "#86181d" }}>
                    <div style={{ fontWeight: 700 }}>Failed to create manager</div>
                    <div style={{ fontSize: 13 }}>{mgrCreateError}</div>
                  </div>
                )}

                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 12 }}>
                  <button onClick={() => { setManagerModalOpen(false); setMgrCreatedInfo(null); setMgrCreateError(null); }} style={{ padding: "8px 12px" }}>Cancel</button>
                  <button
                    type="button"
                    onClick={handleCreateManager}
                    disabled={mgrSubmitting}
                    style={{ padding: "8px 12px", background: "#5b4dfd", color: "#fff", borderRadius: 8 }}
                  >
                    {mgrSubmitting ? "Creating…" : "Create"}
                  </button>
                </div>
              </div>
            </div>
          )}


        {/* Branch modal */}
        {branchModalOpen && (
          <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}>
            <div style={{ width: 680, maxHeight: "80vh", overflowY: "auto", background: "#fff", padding: 18, borderRadius: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <h3 style={{ margin: 0 }}>Branches in {selectedRegion}</h3>
                <button onClick={() => setBranchModalOpen(false)}>Close</button>
              </div>
              <div style={{ marginTop: 12 }}>
                {modalBranches.length === 0 ? <div style={{ color: PALETTE.neutral }}>No branches found for this region.</div> : modalBranches.map(b => (
                  <div key={b.branch_id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: 8, borderBottom: "1px solid #f1f5f9" }}>
                    <div>
                      <div style={{ fontWeight: 700 }}>{b.branch_id}</div>
                      <div style={{ color: PALETTE.neutral, fontSize: 13 }}>{b.state} {b.city ? `• ${b.city}` : ""}</div>
                    </div>
                    <div><button onClick={() => { setSelectedBranch(b.branch_id); setBranchModalOpen(false); }}>Open</button></div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
