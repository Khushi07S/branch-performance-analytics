import React, { useEffect, useState } from 'react'
import jwtDecode from 'jwt-decode'
import LoginScreen from './screens/LoginScreen.jsx'
import PasswordReset from './screens/PasswordReset.jsx'
import DashboardScreen from './screens/Dashboard.jsx'

// Backend API base (keep using this constant throughout the app)
export const API_BASE_URL = '/api/v1'

function App() {
  // token is stored as string or null
  const [token, setToken] = useState(() => localStorage.getItem('access_token') || null)
  const [authStage, setAuthStage] = useState(token ? 'dashboard' : 'login')
  const [error, setError] = useState(null)

  // keep token in localStorage (single source of truth)
  useEffect(() => {
    if (token) {
      localStorage.setItem('access_token', token)
      setAuthStage('dashboard')
    } else {
      localStorage.removeItem('access_token')
      setAuthStage('login')
    }
  }, [token])

  // optional: expose decoded claims for convenience (role/branch)
  const userClaims = React.useMemo(() => {
    if (!token) return null
    try {
      // jwtDecode may throw if token malformed
      return jwtDecode(token)
    } catch (err) {
      return null
    }
  }, [token])

  // helper: logout convenience
  const logout = () => {
    setToken(null)
    setError(null)
    setAuthStage('login')
    localStorage.removeItem('access_token')
  }

  // Pick which top-level component to render
  let ComponentToRender = null
  switch (authStage) {
    case 'login':
      ComponentToRender = <LoginScreen setToken={setToken} setAuthStage={setAuthStage} setError={setError} />
      break
    case 'reset_password':
      ComponentToRender = <PasswordReset setToken={setToken} setAuthStage={setAuthStage} setError={setError} />
      break
    case 'dashboard':
      // require a token for dashboard
      ComponentToRender = token ? (
        <DashboardScreen 
          token={token} 
          setToken={setToken} 
          setError={setError} 
          logout={logout} 
        />
      ) : (
        <LoginScreen 
          setToken={setToken} 
          setAuthStage={setAuthStage} 
          setError={setError} 
        />
      )
      break
    default:
      ComponentToRender = <LoginScreen setToken={setToken} setAuthStage={setAuthStage} setError={setError} />
  }

  return (
    <div 
      style={{ 
        minHeight: '100vh', 
        width: '100%',
        background: authStage === 'dashboard' ? '#f3f4f6' : '#f9fafb',
        display: 'flex',
        alignItems: authStage === 'dashboard' ? 'stretch' : 'center',
        justifyContent: authStage === 'dashboard' ? 'flex-start' : 'center'
      }}
    >
      {/* top-left error area */}
      {error && (
        <div style={{ position: 'absolute', top: 12, left: 12, zIndex: 40 }}>
          <div style={{ 
            background: '#fee2e2', 
            color: '#7f1d1d', 
            padding: '8px 12px', 
            borderRadius: 8, 
            fontWeight: 600 
          }}>
            {error}
          </div>
        </div>
      )}

      {/* Render the selected screen */}
      {ComponentToRender}
    </div>
  )
}

export default App