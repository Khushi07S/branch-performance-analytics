// frontend/src/screens/LoginScreen.jsx
import React, { useState } from "react";
import axios from "axios";
import InputField from "../ui/InputField.jsx";

const API_BASE_URL = import.meta.env.VITE_API_BASE;
console.log("💡 Using API:", API_BASE_URL);
export default function LoginScreen({ setToken, setAuthStage, setError }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const resp = await axios.post(
  `${API_BASE_URL}/auth/login`,
  { username, password },
  { headers: { "Content-Type": "application/json" } }
);

const { access_token, needs_reset, reset_token, user } = resp.data;

// If backend sends user in a nested object (user: {...})
// otherwise adjust to resp.data.role if that's how you return it.
const role = user?.role || resp.data.role;

// 1) Admin: always go straight to dashboard
if (role === "admin") {
  setToken(access_token);
}
// 2) Manager (or others) that must reset password first
else if (needs_reset) {
  if (reset_token) {
    sessionStorage.setItem("reset_token", reset_token);
  }
  setAuthStage("reset_password");
}
// 3) Normal login -> dashboard
else {
  setToken(access_token);
}

    }  catch (err) {
      console.error("Login error", err.response?.status, err.response?.data, err);
      setError(err.response?.data?.msg || `Login failed (${err.response?.status || err.message})`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ minHeight: "80vh", display: "flex", alignItems: "center", justifyContent: "center", background: "#f8fafc", padding: 24 }}>
      <div style={{ width: 520, background: "#ffffff", padding: 32, borderRadius: 12, boxShadow: "0 10px 30px rgba(2,6,23,0.08)" }}>
        <div style={{ textAlign: "center", marginBottom: 18 }}>
          <h1 style={{ margin: 0, color: "#0f172a" }}>AAASK — Analytics Solution</h1>
          <p style={{ marginTop: 6, color: "#475569" }}>Admin / Manager access</p>
        </div>

        <form onSubmit={handleSubmit} style={{ display: "grid", gap: 12 }}>
          <InputField label="Username" type="text" value={username} onChange={(e) => setUsername(e.target.value)} required placeholder="Your username" />
          {/* Turn on debug only if you still can't see eye icon */}
          <InputField label="Password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} required placeholder="Your password"/>
          <button
            type="submit"
            disabled={submitting}
            style={{
              marginTop: 8,
              padding: "12px 16px",
              background: "#0ea5a4",
              color: "#fff",
              borderRadius: 8,
              border: "none",
              fontWeight: 600,
              cursor: "pointer",
            }}
          >
            {submitting ? "Signing in..." : "Sign in"}
          </button>
        </form>

        <div style={{ marginTop: 12, fontSize: 13, color: "#64748b", textAlign: "center" }}>
          <small>Need an account? Contact your admin (only admin can create manager accounts).</small>
        </div>
      </div>
    </div>
  );
}
