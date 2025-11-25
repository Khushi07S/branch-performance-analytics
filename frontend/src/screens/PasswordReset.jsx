import React, { useState } from 'react'
import axios from 'axios'
import { API_BASE_URL } from '../App.jsx'

export default function PasswordReset({ setToken, setAuthStage, setError }) {
  const [oldPassword, setOldPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const resetToken = sessionStorage.getItem('reset_token')
  const client = axios.create({ baseURL: API_BASE_URL })

  const submit = async (e) => {
    e.preventDefault()
    setError(null)
    if (newPassword !== confirm) return setError('New passwords do not match')
    try {
      const res = await client.post('/auth/reset-password', {
        old_password: oldPassword,
        new_password: newPassword
      }, {
        headers: { Authorization: `Bearer ${resetToken}` }
      })
      setToken(res.data.access_token)
      sessionStorage.removeItem('reset_token')
    } catch (err) {
      setError(err.response?.data?.msg || 'Reset failed')
    }
  }

  return (
    <div className="w-full max-w-md mx-auto bg-slate-800 p-8 rounded-xl shadow-xl">
      <h2 className="text-xl font-semibold text-yellow-300 mb-4">Password Reset</h2>
      <form onSubmit={submit} className="space-y-4">
        <input value={oldPassword} onChange={e => setOldPassword(e.target.value)} placeholder="Current password" type="password" className="w-full p-2 bg-slate-700 rounded" required />
        <input value={newPassword} onChange={e => setNewPassword(e.target.value)} placeholder="New password" type="password" className="w-full p-2 bg-slate-700 rounded" required />
        <input value={confirm} onChange={e => setConfirm(e.target.value)} placeholder="Confirm new password" type="password" className="w-full p-2 bg-slate-700 rounded" required />
        <button className="w-full py-2 bg-yellow-500 hover:bg-yellow-600 rounded">Set New Password</button>
      </form>
    </div>
  )
}
