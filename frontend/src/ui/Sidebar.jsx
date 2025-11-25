// frontend/src/ui/Sidebar.jsx
import React from "react";

/*
  Lightweight Sidebar that does not require react-router-dom.
  If you prefer react-router navigation, install react-router-dom and
  uncomment the useNavigate import + related code.
*/

export default function Sidebar() {
  // If you later want to use react-router, uncomment:
  // import { useNavigate } from "react-router-dom";
  // const navigate = useNavigate();

  function handleSignOut() {
    try {
      localStorage.removeItem("access_token");
      localStorage.removeItem("token");
      // clear other app state if any
    } catch (e) {
      // ignore
    }
    // If you have client-side router and want SPA navigation:
    // if (navigate) { navigate("/login", { replace: true }); return; }
    // Fallback to full redirect which works without router:
    window.location.href = "/login";
  }

  return (
    <div style={{
      padding: 18,
      background: "#fff",
      height: "100vh",
      boxShadow: "2px 0 12px rgba(15,30,60,0.03)",
      position: "relative"
    }}>
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontWeight: 800 }}>BharatXCorp</div>
        <div style={{ color: "#64748b", fontSize: 13 }}>Branch Analytics</div>
      </div>

      <nav style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        <a href="/dashboard" style={{ color: "#0f172a", fontWeight: 700, textDecoration: "none" }}>Dashboard</a>
        <a href="/performance" style={{ color: "#64748b", textDecoration: "none" }}>Performance</a>
        <a href="/locations" style={{ color: "#64748b", textDecoration: "none" }}>Locations</a>
        <a href="/settings" style={{ color: "#64748b", textDecoration: "none" }}>Settings</a>
      </nav>

      <div style={{ position: "absolute", bottom: 24, left: 18 }}>
        <div style={{ color: "#64748b", fontSize: 12 }}>Signed in as</div>
        <div style={{ fontWeight: 700 }}>User</div>
        <div style={{ marginTop: 6 }}>
          <button
            onClick={handleSignOut}
            style={{
              background: "none",
              border: "none",
              color: "#6366f1",
              cursor: "pointer",
              padding: 6,
              fontSize: 14
            }}
          >
            Sign out
          </button>
        </div>
      </div>
    </div>
  );
}
