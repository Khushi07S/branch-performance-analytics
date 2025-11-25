// frontend/src/ui/InputField.jsx
import React, { useState } from "react";

export default function InputField({
  label,
  type = "text",
  value,
  onChange,
  placeholder = "",
  required = false,
  name,
  id,
}) {
  const [visible, setVisible] = useState(false);
  const isPassword = type === "password";

  const inputType = isPassword ? (visible ? "text" : "password") : type;

  return (
    <label style={{ display: "block", fontSize: 14, color: "#334155" }}>
      <div style={{ marginBottom: 8, fontWeight: 500 }}>{label}</div>
      <div style={{ position: "relative" }}>
        <input
          id={id}
          name={name}
          value={value}
          onChange={onChange}
          placeholder={placeholder}
          required={required}
          type={inputType}
          style={{
            width: "100%",
            padding: isPassword ? "12px 44px 12px 12px" : "12px",
            borderRadius: 8,
            border: "1px solid #e6edf3",
            outline: "none",
            boxSizing: "border-box",
            fontSize: 15,
            color: "#0f172a",
            background: "#fff",
          }}
        />
        {isPassword && (
          <button
            type="button"
            aria-label={visible ? "Hide password" : "Show password"}
            onClick={() => setVisible((s) => !s)}
            style={{
              position: "absolute",
              right: 8,
              top: "50%",
              transform: "translateY(-50%)",
              border: "none",
              background: "transparent",
              padding: 6,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            {/* simple inline icons: eye (when password is hidden) and eye-off (when visible) */}
            {visible ? (
              // eye-off (password visible -> show eye-off to indicate clicking will hide)
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden>
                <path d="M3 3l18 18" stroke="#1f2937" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
                <path d="M10.47 10.47a3 3 0 0 0 4.06 4.06" stroke="#1f2937" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
                <path d="M9.88 5.09A16.88 16.88 0 0 1 12 4.5c5 0 9.26 3 11 7.5a16.88 16.88 0 0 1-2.31 4.25" stroke="#1f2937" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
            ) : (
              // eye (password hidden -> show eye to indicate clicking will reveal)
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden>
                <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z" stroke="#1f2937" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
                <circle cx="12" cy="12" r="3" stroke="#1f2937" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
            )}
          </button>
        )}
      </div>
    </label>
  );
}
