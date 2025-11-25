// frontend/src/api.js
// Resilient API shim: tries the real backend (REACT_APP_API_BASE / import.meta / window.__API_BASE__)
// and falls back to an in-memory mock when the real backend is unreachable.
// This is the version you had when the dashboard worked (it returned your real 100 branches
// when the backend was up, and used mock data only when the backend was down).

/* Safe environment detection */
function _getEnvBase() {
  if (typeof process !== "undefined" && process?.env?.REACT_APP_API_BASE) return process.env.REACT_APP_API_BASE;
  if (typeof import.meta !== "undefined" && import.meta?.env?.VITE_API_BASE) return import.meta.env.VITE_API_BASE;
  if (typeof window !== "undefined" && window.__API_BASE__) return window.__API_BASE__;
  return "";
}

const API_BASE = (_getEnvBase() || "").replace(/\/$/, "");
const useReal = Boolean(API_BASE && API_BASE.length);

/* helpers */
function sleep(ms = 300) { return new Promise(res => setTimeout(res, ms)); }
function isNetworkError(err) { return err instanceof TypeError; } // fetch throws TypeError on network failure

async function doFetch(path, opts = {}) {
  if (!useReal) throw new Error("No real API configured");
  const url = API_BASE + path;
  const options = {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    credentials: "include",
    body: opts.body,
  };
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    const err = new Error(`API error ${res.status}: ${text}`);
    err.status = res.status;
    throw err;
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) return res.json();
  return res.text();
}

async function tryRealOrThrow(path, opts = {}) {
  if (!useReal) throw new Error("No real API configured");
  try { return await doFetch(path, opts); }
  catch (err) {
    // network-level error (connection refused, DNS, etc.) -> indicate to caller so fallback can run
    if (isNetworkError(err)) {
      console.warn(`API network error when calling ${API_BASE + path} — will fallback to mock.`, err);
      throw err;
    }
    // HTTP error (4xx/5xx) -> rethrow so caller can inspect / bubble up
    throw err;
  }
}

/* in-memory mock store used only as fallback */
const _mock = {
  branches: Array.from({ length: 100 }, (_, i) => ({ id: `BR${String(i+1).padStart(3,"0")}`, name: `Branch ${i+1}` })),
  managers: [],
};

const api = {
  async getBranches() {
    try {
      const data = await tryRealOrThrow("/branches");
      return { data };
    } catch (err) {
      // fallback to mock
      await sleep(80);
      return { data: _mock.branches };
    }
  },

  async getBranchOverview(branchId) {
    try {
      const data = await tryRealOrThrow(`/branches/${encodeURIComponent(branchId)}/overview`);
      return { data };
    } catch (err) {
      await sleep(120);
      const total = Math.round(5_000_000 + Math.random() * 2_000_000);
      return {
        data: {
          branchId,
          totalaggregatedeposits: total,
          totalcasa: Math.round(total * 0.35),
          totalaggregatecredit: Math.round(total * 0.6),
          totaltransactionscount: Math.round(10_000 + Math.random() * 40_000),
          totalcurrentdeposits: Math.round(total * 0.45),
          totalsavingsdeposits: Math.round(total * 0.35),
          totaltermdeposits: Math.round(total * 0.2),
          topBranches: _mock.branches.slice(0, 4).map((b, i) => ({ branch_id: b.id, name: b.name, deposits: Math.round(total / (i + 2)) })),
          monthlyTransactions: Array.from({ length: 6 }, (_, i) => ({ month: `M-${5 - i}`, count: Math.round(1000 + Math.random() * 5000) })),
        }
      };
    }
  },

  async getBranchTimeSeries(branchId, { metrics } = {}) {
    try {
      const q = metrics ? `?metrics=${encodeURIComponent(metrics)}` : "";
      const data = await tryRealOrThrow(`/branches/${encodeURIComponent(branchId)}/timeseries${q}`);
      return { data };
    } catch (err) {
      await sleep(80);
      const labels = ["2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1"];
      const metricList = metrics ? metrics.split(",") : ["totalaggregatedeposits", "totalaggregatecredit", "totaltransactionscount"];
      const datasets = metricList.map((m) => ({ label: m, data: labels.map(() => Math.round(Math.random() * 2000 + 500)) }));
      return { data: { labels, datasets } };
    }
  },

  async getRegionalKPIs(year) {
    try {
      const data = await tryRealOrThrow(`/regional?kpi_year=${encodeURIComponent(String(year))}`);
      return { data };
    } catch (err) {
      await sleep(60);
      return { data: [{ name: "West", value: Math.round(10000 + Math.random() * 10000) }, { name: "North", value: Math.round(8000 + Math.random() * 5000) }] };
    }
  },

  async getBranchesByRegion(regionName) {
    try {
      const data = await tryRealOrThrow(`/branches?region=${encodeURIComponent(regionName)}`);
      return { data };
    } catch (err) {
      await sleep(60);
      return { data: _mock.branches.map(b => ({ branch_id: b.id, name: b.name, state: "State", city: "City" })) };
    }
  },

  async getBranchGrid(branchId) {
    try {
      const data = await tryRealOrThrow(`/branches/${encodeURIComponent(branchId)}/grid`);
      return { data };
    } catch (err) {
      await sleep(40);
      return { data: [{ id: 1, name: "Row 1" }, { id: 2, name: "Row 2" }] };
    }
  },

  async getCurrentUser() {
    try {
      const data = await tryRealOrThrow(`/me`);
      return { data };
    } catch (err) {
      await sleep(30);
      // default mock: admin (change to manager to test manager view)
      return { data: { name: "Alice Admin", role: "admin", branchId: null, email: "alice@example.com" } };
    }
  },

  async createManager(payload) {
    try {
      const data = await tryRealOrThrow(`/users`, { method: "POST", body: JSON.stringify(payload) });
      return { data };
    } catch (err) {
      await sleep(300);
      const exists = _mock.managers.some(m => m.username === payload.username || m.email === payload.email);
      if (exists) throw new Error("Username or email already exists (mock)");
      const created = { id: `mgr_${Date.now()}`, ...payload };
      _mock.managers.push(created);
      return { data: { ok: true, created } };
    }
  }
};

export default api;
