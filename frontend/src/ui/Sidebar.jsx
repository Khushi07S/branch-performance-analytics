// frontend/src/ui/Sidebar.jsx
import React from "react";

/*
  Sidebar Component
  
  Props:
    - userName: string (display name of logged-in user)
    - userRole: string (optional, shows role badge)
    - onSignOut: function (callback for sign out)
*/

export default function Sidebar({ 
  userName = "User", 
  userRole = null,
  onSignOut 
}) {
  function handleSignOut() {
    // Prefer parent's logout handler
    if (typeof onSignOut === "function") {
      onSignOut();
      return;
    }

    // Fallback: manual clear
    try {
      localStorage.removeItem("access_token");
      localStorage.removeItem("token");
    } catch (e) {
      console.warn("Failed to clear storage:", e);
    }
    window.location.href = "/login";
  }

  const navItemStyle = (active = false) => ({
    padding: "10px 14px",
    borderRadius: 10,
    fontSize: 14,
    textDecoration: "none",
    display: "flex",
    alignItems: "center",
    gap: 10,
    color: active ? "#111827" : "#6b7280",
    fontWeight: active ? 600 : 500,
    background: active ? "#eef2ff" : "transparent",
    transition: "all 150ms ease",
  });

  const iconStyle = {
    width: 18,
    height: 18,
    opacity: 0.7,
  };

  return (
    <div
      style={{
        padding: "20px 16px",
        background: "#ffffff",
        height: "100vh",
        boxShadow: "2px 0 12px rgba(15,30,60,0.05)",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Brand Header */}
      <div style={{ marginBottom: 28, paddingLeft: 4 }}>
        <div
          style={{
            fontWeight: 800,
            fontSize: 19,
            letterSpacing: 0.3,
            color: "#0f172a",
          }}
        >
          BharatXCorp
        </div>
        <div
          style={{
            color: "#64748b",
            fontSize: 12,
            marginTop: 4,
            fontWeight: 500,
          }}
        >
          Branch Performance Analytics
        </div>
      </div>

      {/* Navigation Links */}
      <nav style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        <a href="/dashboard" style={navItemStyle(true)}>
          <span style={iconStyle}>📊</span>
          Dashboard
        </a>
        <a href="/performance" style={navItemStyle(false)}>
          <span style={iconStyle}>📈</span>
          Performance
        </a>
        <a href="/locations" style={navItemStyle(false)}>
          <span style={iconStyle}>📍</span>
          Locations
        </a>
        <a href="/settings" style={navItemStyle(false)}>
          <span style={iconStyle}>⚙️</span>
          Settings
        </a>
      </nav>

      {/* Spacer to push footer down */}
      <div style={{ flex: 1 }} />

      {/* User Profile Footer */}
      <div
        style={{
          marginTop: 16,
          padding: "14px 12px",
          background: "linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)",
          borderRadius: 12,
          border: "1px solid #e2e8f0",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            marginBottom: 10,
          }}
        >
          {/* User Avatar */}
          <div
            style={{
              width: 36,
              height: 36,
              borderRadius: "50%",
              background: "linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "#fff",
              fontWeight: 700,
              fontSize: 14,
            }}
          >
            {userName.charAt(0).toUpperCase()}
          </div>
          
          <div style={{ flex: 1, minWidth: 0 }}>
            <div
              style={{
                fontWeight: 700,
                color: "#0f172a",
                fontSize: 14,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {userName}
            </div>
            {userRole && (
              <div
                style={{
                  fontSize: 11,
                  color: "#64748b",
                  textTransform: "capitalize",
                  marginTop: 2,
                }}
              >
                {userRole}
              </div>
            )}
          </div>
        </div>

        <button
          onClick={handleSignOut}
          style={{
            width: "100%",
            padding: "8px 12px",
            background: "#fff",
            border: "1px solid #e2e8f0",
            borderRadius: 8,
            color: "#6366f1",
            cursor: "pointer",
            fontSize: 13,
            fontWeight: 600,
            transition: "all 150ms ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#f8fafc";
            e.currentTarget.style.borderColor = "#6366f1";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "#fff";
            e.currentTarget.style.borderColor = "#e2e8f0";
          }}
        >
          Sign out
        </button>
      </div>
    </div>
  );
}