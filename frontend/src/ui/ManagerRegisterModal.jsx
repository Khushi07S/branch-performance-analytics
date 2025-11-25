// frontend/src/ui/ManagerRegisterModal.jsx
import React, { useState, useEffect } from "react";
import api from "../api";

/**
 * Props:
 *  - open (bool)
 *  - onClose() called when modal closes
 *  - onCreated(managerInfo) optional callback after success
 *  - branches: optional array [{ id, name }] to populate branch selector
 */
export default function ManagerRegisterModal({ open, onClose, branches = [], onCreated = () => {} }) {
  const [username, setUsername] = useState("");
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [branchId, setBranchId] = useState(branches?.[0]?.id ?? "");
  const [tempPassword, setTempPassword] = useState(""); // optional: admin-provided temp pass
  const [autoGenerate, setAutoGenerate] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [successMessage, setSuccessMessage] = useState("");

  useEffect(() => {
    // default branch when modal opens
    if (open && branches && branches.length > 0) {
      setBranchId(branches[0].id ?? branches[0].branchId ?? "");
    }
    if (!open) {
      setUsername(""); setFullName(""); setEmail(""); setTempPassword(""); setError(""); setSuccessMessage("");
    }
  }, [open, branches]);

  function validate() {
    if (!username.trim()) { setError("Username required"); return false; }
    if (!fullName.trim()) { setError("Full name required"); return false; }
    if (!email.trim() || !/^\S+@\S+\.\S+$/.test(email)) { setError("Valid email required"); return false; }
    if (!branchId) { setError("Select a branch"); return false; }
    if (!autoGenerate && tempPassword.length < 6) { setError("Temporary password must be at least 6 characters"); return false; }
    setError("");
    return true;
  }

  async function handleSubmit(e) {
    e?.preventDefault?.();
    if (!validate()) return;
    setLoading(true);
    setError("");
    setSuccessMessage("");

    try {
      const payload = {
        username: username.trim(),
        name: fullName.trim(),
        email: email.trim(),
        branchId,
        // prefer letting backend generate; if admin provided, pass it.
        ...(autoGenerate ? {} : { tempPassword }),
        forcePasswordChange: true, // instruct backend to mark "must change password on first login"
      };

      const res = await api.createManager(payload);
      // Expect backend 201 created. Some backends may return the temporary password in dev mode (not recommended in prod)
      setSuccessMessage("Manager created successfully. A temporary password was sent to the manager's email.");
      onCreated(res.data);
    } catch (err) {
      console.error("createManager error:", err);
      if (err?.response?.data?.msg) {
        setError(err.response.data.msg);
      } else if (err?.response?.status === 409) {
        setError("Username or email already exists.");
      } else {
        setError("Failed to create manager. Check console/network.");
      }
    } finally {
      setLoading(false);
    }
  }

  if (!open) return null;

  return (
    <div style={{
      position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", display: "flex", alignItems: "center", justifyContent: "center",
      zIndex: 9999
    }}>
      <div style={{ width: 520, background: "#fff", padding: 20, borderRadius: 10 }}>
        <h3 style={{ marginTop: 0 }}>Register New Branch Manager</h3>
        <form onSubmit={handleSubmit}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <input placeholder="Username" value={username} onChange={(e) => setUsername(e.target.value)} />
            <input placeholder="Full name" value={fullName} onChange={(e) => setFullName(e.target.value)} />
            <input placeholder="Email" type="email" value={email} onChange={(e) => setEmail(e.target.value)} />
            <select value={branchId} onChange={(e) => setBranchId(e.target.value)}>
              <option value="">-- Select branch --</option>
              {branches.map((b) => <option key={b.id ?? b.branchId ?? b.name} value={b.id ?? b.branchId ?? b.name}>{b.name ?? b.displayName ?? b.id}</option>)}
            </select>
          </div>

          <div style={{ marginTop: 12 }}>
            <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <input type="checkbox" checked={autoGenerate} onChange={() => setAutoGenerate(!autoGenerate)} />
              Auto-generate temporary password (recommended)
            </label>
            {!autoGenerate && (
              <div style={{ marginTop: 8 }}>
                <input placeholder="Temporary password (min 6 chars)" value={tempPassword} onChange={(e) => setTempPassword(e.target.value)} />
              </div>
            )}
          </div>

          {error && <div style={{ color: "crimson", marginTop: 10 }}>{error}</div>}
          {successMessage && <div style={{ color: "green", marginTop: 10 }}>{successMessage}</div>}

          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 14 }}>
            <button type="button" onClick={onClose} disabled={loading}>Cancel</button>
            <button type="submit" style={{ padding: "8px 12px" }} disabled={loading}>
              {loading ? "Creating..." : "Create Manager"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
