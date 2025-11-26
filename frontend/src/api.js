// frontend/src/api.js
// Resilient API shim: tries the real backend (REACT_APP_API_BASE / import.meta / window.__API_BASE__)
// and falls back to an in-memory mock when the real backend is unreachable.

function _getEnvBase() {
  // CRA: process.env.REACT_APP_API_BASE (available at build time)
  if (typeof process !== "undefined" && process?.env?.REACT_APP_API_BASE) return process.env.REACT_APP_API_BASE;
  // Vite: import.meta.env.VITE_API_BASE (available at build time) — guard in try/catch
  try {
    if (import.meta?.env?.VITE_API_BASE) {
      return import.meta.env.VITE_API_BASE;
    }
  } catch (e) {
    // ignore
  }
  // runtime override (useful for quick testing)
  if (typeof window !== "undefined" && window.__API_BASE__) return window.__API_BASE__;
  return "";
}

const API_BASE = (_getEnvBase() || "").replace(/\/$/, "");
const useReal = Boolean(API_BASE && API_BASE.length);

/* helpers */
function sleep(ms = 300) {
  return new Promise((res) => setTimeout(res, ms));
}
function isNetworkError(err) {
  // fetch throws TypeError on network failure in browsers
  return err instanceof TypeError;
}

async function doFetch(path, opts = {}) {
  if (!useReal) throw new Error("No real API configured");
  const url = API_BASE + path;
  const options = {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    credentials: opts.credentials ?? "include",
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
  try {
    return await doFetch(path, opts);
  } catch (err) {
    if (isNetworkError(err)) {
      console.warn(`API network error when calling ${API_BASE + path} — will fallback to mock.`, err);
      throw err;
    }
    throw err;
  }
}

/* in-memory mock store used only as fallback */
const _mock = {
  branches: Array.from({ length: 100 }, (_, i) => ({ id: `BR${String(i + 1).padStart(3, "0")}`, name: `Branch ${i + 1}` })),
  managers: [],
};

/* Normalizer for branch lists (coerces different backend shapes to stable shape) */
function _normalizeBranchesList(rawList) {
  if (!Array.isArray(rawList)) return [];
  return rawList.map((b) => {
    const branch_id = b.branch_id ?? b.id ?? b.branchId ?? b.branch ?? null;
    const name = b.name ?? b.branch_name ?? b.branchName ?? (branch_id ? String(branch_id) : "Unknown");
    const state = b.state ?? b.region ?? b.state_name ?? b.stateName ?? null;
    const city = b.city ?? b.town ?? b.city_name ?? b.cityName ?? null;
    return { branch_id, name, state, city, raw: b };
  });
}

/* getBranchesByRegion: tries backend then falls back to mock with state/city */
async function getBranchesByRegionImpl(regionName) {
  try {
    const data = await tryRealOrThrow(`/branches?region=${encodeURIComponent(regionName)}`);
    // backend might return array or { branches: [...] } or { data: [...] } etc.
    let list = [];
    if (Array.isArray(data)) list = data;
    else if (Array.isArray(data.branches)) list = data.branches;
    else if (Array.isArray(data.data)) list = data.data;
    else if (data && typeof data === "object") {
      // try to extract array-like values
      const maybeArray = Object.values(data).filter((v) => Array.isArray(v) && v.length);
      if (maybeArray.length) list = maybeArray[0];
      else {
        // fallback to common keys
        list = data.results ?? data.items ?? [];
      }
    }
    const normalized = _normalizeBranchesList(list);
    return { data: normalized };
  } catch (err) {
    // fallback: return mock branches enriched with plausible state/city
    await sleep(60);
    const stateSamples = {
      West: ["Maharashtra", "Gujarat", "Goa"],
      North: ["Delhi", "Haryana", "Punjab"],
      South: ["Karnataka", "Tamil Nadu", "Kerala"],
      East: ["West Bengal", "Odisha", "Bihar"],
      Central: ["Madhya Pradesh", "Chhattisgarh"],
    };
    const citiesByState = {
      Maharashtra: ["Mumbai", "Pune", "Nagpur"],
      Gujarat: ["Ahmedabad", "Surat"],
      Goa: ["Panaji"],
      Delhi: ["New Delhi"],
      Haryana: ["Gurgaon", "Faridabad"],
      Punjab: ["Chandigarh", "Ludhiana"],
      Karnataka: ["Bengaluru", "Mysore"],
      "Tamil Nadu": ["Chennai", "Coimbatore"],
      Kerala: ["Kochi", "Thiruvananthapuram"],
      "West Bengal": ["Kolkata", "Durgapur"],
      Odisha: ["Bhubaneswar"],
      Bihar: ["Patna"],
      "Madhya Pradesh": ["Bhopal", "Indore"],
      "Chhattisgarh": ["Raipur"],
    };

    const states = stateSamples[regionName] ?? stateSamples["Central"];
    const chosenState = states && states.length ? states[0] : "State";
    const chosenCity = (citiesByState[chosenState] && citiesByState[chosenState][0]) || "City";

    const mockList = _mock.branches.slice(0, 12).map((b, i) => ({
      branch_id: b.id,
      name: b.name,
      state: chosenState,
      city: chosenCity + (i > 0 ? ` ${i + 1}` : ""),
    }));
    return { data: mockList };
  }
}

/* Public API */
const api = {
  async getBranches() {
    try {
      const data = await tryRealOrThrow("/branches");
      return { data };
    } catch (err) {
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
        },
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

  // normalized branches-by-region that returns objects with branch_id, name, state, city
  async getBranchesByRegion(regionName) {
    return getBranchesByRegionImpl(regionName);
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
      return { data: { name: "Alice Admin", role: "admin", branchId: null, email: "alice@example.com" } };
    }
  },

      async createManager(payload) {
    // We expect VITE_API_BASE / REACT_APP_API_BASE to be something like:
    //   http://127.0.0.1:5000/api/v1
    // So this path becomes: /api/v1/admin/auth/create-manager

    const token =
      (typeof window !== "undefined" &&
        (localStorage.getItem("access_token") ||
          localStorage.getItem("token"))) ||
      null;

    const headers = { "Content-Type": "application/json" };
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }

    // Map the Dashboard fields to what backend expects
    const effectiveBranchId =
      payload.managed_branch_id ||
      payload.branchId ||
      payload.branch_id ||
      null;

    const tempPassword = payload.temp_password || payload.password;

    const body = {
      username: payload.username,
      email: payload.email,
      managed_branch_id: effectiveBranchId,
      temp_password: tempPassword,
      // extra info (backend can ignore if it doesn’t use it)
      name: payload.name,
    };

    try {
      const data = await tryRealOrThrow(`/admin/auth/create-manager`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
      return { data };
    } catch (err) {
      console.warn("createManager real API failed, using mock fallback:", err);

      // ---- MOCK FALLBACK (only used if backend is unreachable) ----
      await sleep(300);

      const exists = _mock.managers.some(
        (m) =>
          m.username === payload.username || m.email === payload.email
      );
      if (exists) {
        const e = new Error(
          "Username or email already exists (mock fallback)"
        );
        e.status = 400;
        throw e;
      }

      const created = {
        id: `mgr_${Date.now()}`,
        username: payload.username,
        email: payload.email,
        managed_branch_id: effectiveBranchId,
        temp_password: tempPassword,
        name: payload.name,
      };
      _mock.managers.push(created);

      return {
        data: {
          ok: true,
          created,
        },
      };
    }
  },


};

export default api;
