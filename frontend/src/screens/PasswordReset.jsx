import React, { useState } from "react";
import axios from "axios";

const API_BASE_URL = import.meta.env.VITE_API_BASE;

export default function PasswordReset({ setAuthStage }) {
  const [newPassword, setNewPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const resetToken = sessionStorage.getItem("reset_token");

  const handleReset = async (e) => {
    e.preventDefault();
    setError(null);

    if (!resetToken) {
      setError("Missing reset token. Please login again.");
      return;
    }

    if (!newPassword.trim()) {
      setError("Password cannot be empty.");
      return;
    }

    if (newPassword !== confirm) {
      setError("Passwords do not match.");
      return;
    }

    setLoading(true);

    console.log("🔐 Using reset token:", resetToken);
    console.log("📡 Reset URL:", `${API_BASE_URL}/auth/reset-password`);

    try {
      const resp = await axios.post(
        `${API_BASE_URL}/auth/reset-password`,
        { new_password: newPassword },               // body
        {
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${resetToken}`, // 🟢 REQUIRED
          },
        }
      );

      console.log("Password reset resp:", resp.data);

      alert("Password updated! Please login again.");
      sessionStorage.removeItem("reset_token");
      setAuthStage("login");

    } catch (err) {
      console.error("Reset error:", err.response?.data || err);
      setError(err.response?.data?.msg || "Failed to reset password");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: 24 }}>
      <h2>Reset Password</h2>

      <form onSubmit={handleReset}>
        <input
          type="password"
          placeholder="New password"
          value={newPassword}
          onChange={(e) => setNewPassword(e.target.value)}
        />

        <input
          type="password"
          placeholder="Confirm password"
          value={confirm}
          onChange={(e) => setConfirm(e.target.value)}
        />

        {error && <p style={{ color: "red" }}>{error}</p>}

        <button type="submit" disabled={loading}>
          {loading ? "Updating..." : "Update Password"}
        </button>
      </form>
    </div>
  );
}
