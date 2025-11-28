import React from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.jsx'
import './styles.css' // optional Tailwind / fallback CSS
import.meta.env.VITE_API_BASE

createRoot(document.getElementById('root')).render(<App />)
