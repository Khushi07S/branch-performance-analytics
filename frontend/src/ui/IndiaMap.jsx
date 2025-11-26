// frontend/src/ui/IndiaMap.jsx
import React, { useEffect, useState, useMemo } from "react";
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup,
} from "react-simple-maps";

const DEFAULT_TOPO_URLS = [
  "https://cdn.jsdelivr.net/gh/udit-001/india-maps-data@8d907bc/topojson/india.json",
];

// Improved color scheme - more vibrant
function valueToColor(val, max) {
  if (!val || !max) return "#e8eef5"; // very light blue-gray for no data

  const ratio = Math.max(0, Math.min(1, val / max));
  
  // Vibrant gradient: light teal → deep blue
  if (ratio < 0.33) {
    // Light range: #a7f3d0 → #6ee7b7
    const start = { r: 167, g: 243, b: 208 };
    const end = { r: 110, g: 231, b: 183 };
    const localRatio = ratio / 0.33;
    return `rgb(${Math.round(start.r + (end.r - start.r) * localRatio)}, ${Math.round(start.g + (end.g - start.g) * localRatio)}, ${Math.round(start.b + (end.b - start.b) * localRatio)})`;
  } else if (ratio < 0.66) {
    // Mid range: #6ee7b7 → #3b82f6
    const start = { r: 110, g: 231, b: 183 };
    const end = { r: 59, g: 130, b: 246 };
    const localRatio = (ratio - 0.33) / 0.33;
    return `rgb(${Math.round(start.r + (end.r - start.r) * localRatio)}, ${Math.round(start.g + (end.g - start.g) * localRatio)}, ${Math.round(start.b + (end.b - start.b) * localRatio)})`;
  } else {
    // High range: #3b82f6 → #1e40af
    const start = { r: 59, g: 130, b: 246 };
    const end = { r: 30, g: 64, b: 175 };
    const localRatio = (ratio - 0.66) / 0.34;
    return `rgb(${Math.round(start.r + (end.r - start.r) * localRatio)}, ${Math.round(start.g + (end.g - start.g) * localRatio)}, ${Math.round(start.b + (end.b - start.b) * localRatio)})`;
  }
}

function normalizeStateName(name) {
  if (!name) return "";
  
  const variations = {
    "andaman and nicobar": "andaman & nicobar islands",
    "andaman & nicobar": "andaman & nicobar islands",
    "dadra and nagar haveli": "dadra & nagar haveli and daman & diu",
    "daman and diu": "dadra & nagar haveli and daman & diu",
    "jammu and kashmir": "jammu & kashmir",
    "nct of delhi": "delhi",
  };
  
  const normalized = name.toString().toLowerCase().trim();
  return variations[normalized] || normalized;
}

function formatCurrency(val) {
  if (val >= 10000000) return `₹${(val / 10000000).toFixed(1)}Cr`;
  if (val >= 100000) return `₹${(val / 100000).toFixed(1)}L`;
  if (val >= 1000) return `₹${(val / 1000).toFixed(1)}K`;
  return `₹${val}`;
}

export default function IndiaMap({
  regions = [],
  onRegionClick = () => {},
  selectedRegion = null,
}) {
  const [topo, setTopo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [hoveredName, setHoveredName] = useState(null);
  const [hoveredData, setHoveredData] = useState(null);
  const [position, setPosition] = useState({ coordinates: [80, 22], zoom: 1 });

  const safeRegions = Array.isArray(regions) ? regions : [];

  useEffect(() => {
    let mounted = true;

    async function loadTopo() {
      try {
        setLoading(true);
        for (const url of DEFAULT_TOPO_URLS) {
          try {
            const resp = await fetch(url);
            if (!resp.ok) continue;
            const json = await resp.json();
            if (mounted) {
              setTopo(json);
              setLoading(false);
              return;
            }
          } catch (e) {
            console.warn("Failed to load from", url, e);
          }
        }
        if (mounted) {
          setTopo(null);
          setLoading(false);
        }
      } catch (e) {
        console.error("Topology load error:", e);
        if (mounted) {
          setTopo(null);
          setLoading(false);
        }
      }
    }

    loadTopo();
    return () => {
      mounted = false;
    };
  }, []);

  const maxVal = safeRegions.length
    ? Math.max(...safeRegions.map((r) => Number(r.deposits || 0)))
    : 0;

  const regionByName = useMemo(() => {
    const map = {};
    safeRegions.forEach((r) => {
      const regionName = r.region || r.state || "";
      const normalized = normalizeStateName(regionName);
      if (normalized) {
        map[normalized] = r;
        const original = regionName.toString().toLowerCase().trim();
        if (original !== normalized) {
          map[original] = r;
        }
      }
    });
    return map;
  }, [safeRegions]);

  function handleZoomIn() {
    if (position.zoom >= 6) return;
    setPosition((pos) => ({ ...pos, zoom: Math.min(6, pos.zoom * 1.5) }));
  }

  function handleZoomOut() {
    if (position.zoom <= 1) return;
    setPosition((pos) => ({ ...pos, zoom: Math.max(1, pos.zoom / 1.5) }));
  }

  function handleReset() {
    setPosition({ coordinates: [80, 22], zoom: 1 });
  }

  if (loading) {
    return (
      <div
        style={{
          height: 300,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "#64748b",
          fontSize: 13,
          background: "#f8fafc",
          borderRadius: 8,
        }}
      >
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: 24, marginBottom: 8 }}>🗺️</div>
          <div>Loading India map…</div>
        </div>
      </div>
    );
  }

  if (!topo) {
    return (
      <div
        style={{
          height: 300,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "#64748b",
          fontSize: 13,
          textAlign: "center",
          padding: "0 12px",
          background: "#f8fafc",
          borderRadius: 8,
        }}
      >
        <div>
          <div style={{ fontSize: 24, marginBottom: 8 }}>⚠️</div>
          <div>Map data could not be loaded.</div>
          <div style={{ fontSize: 11, marginTop: 4 }}>Check your internet connection</div>
        </div>
      </div>
    );
  }

  return (
    <div
      style={{
        width: "100%",
        height: 320,
        display: "flex",
        flexDirection: "column",
        position: "relative",
        background: "#f8fafc",
        borderRadius: 8,
        padding: 12,
      }}
    >
      {/* Zoom controls */}
      <div
        style={{
          position: "absolute",
          top: 16,
          right: 16,
          zIndex: 100,
          display: "flex",
          flexDirection: "column",
          gap: 6,
          background: "rgba(255, 255, 255, 0.95)",
          borderRadius: 8,
          padding: 6,
          boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
        }}
      >
        <button
          onClick={handleZoomIn}
          style={{
            width: 32,
            height: 32,
            border: "1px solid #e2e8f0",
            background: "#fff",
            borderRadius: 6,
            cursor: "pointer",
            fontSize: 16,
            fontWeight: 700,
            color: "#475569",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            transition: "all 150ms",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#f1f5f9";
            e.currentTarget.style.borderColor = "#94a3b8";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "#fff";
            e.currentTarget.style.borderColor = "#e2e8f0";
          }}
        >
          +
        </button>
        <button
          onClick={handleZoomOut}
          style={{
            width: 32,
            height: 32,
            border: "1px solid #e2e8f0",
            background: "#fff",
            borderRadius: 6,
            cursor: "pointer",
            fontSize: 16,
            fontWeight: 700,
            color: "#475569",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            transition: "all 150ms",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#f1f5f9";
            e.currentTarget.style.borderColor = "#94a3b8";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "#fff";
            e.currentTarget.style.borderColor = "#e2e8f0";
          }}
        >
          −
        </button>
        <button
          onClick={handleReset}
          style={{
            width: 32,
            height: 32,
            border: "1px solid #e2e8f0",
            background: "#fff",
            borderRadius: 6,
            cursor: "pointer",
            fontSize: 14,
            color: "#475569",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            transition: "all 150ms",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = "#f1f5f9";
            e.currentTarget.style.borderColor = "#94a3b8";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = "#fff";
            e.currentTarget.style.borderColor = "#e2e8f0";
          }}
          title="Reset zoom"
        >
          ⟲
        </button>
      </div>

      <div style={{ flex: 1, position: "relative", overflow: "hidden", borderRadius: 6 }}>
        <ComposableMap
          projection="geoMercator"
          projectionConfig={{ scale: 1000, center: [80, 22] }}
          style={{ width: "100%", height: "100%" }}
        >
          <ZoomableGroup
            zoom={position.zoom}
            center={position.coordinates}
            onMoveEnd={setPosition}
            minZoom={1}
            maxZoom={6}
          >
            <Geographies geography={topo}>
              {({ geographies }) =>
                geographies.map((geo) => {
                  const rawName =
                    geo.properties?.NAME_1 ||
                    geo.properties?.name ||
                    geo.properties?.st_nm ||
                    geo.id ||
                    "unknown";
                  
                  const normalized = normalizeStateName(rawName);
                  const regionEntry = regionByName[normalized];
                  const val = Number(regionEntry?.deposits || 0);
                  const fill = valueToColor(val, maxVal);

                  const isSelected =
                    selectedRegion &&
                    normalizeStateName(selectedRegion) === normalized;

                  return (
                    <Geography
                      key={geo.rsmKey || geo.geojsonId || geo.id || rawName}
                      geography={geo}
                      onClick={() => {
                        const displayName = regionEntry?.region || 
                                          regionEntry?.state || 
                                          rawName;
                        onRegionClick(displayName);
                      }}
                      onMouseEnter={() => {
                        setHoveredName(rawName);
                        setHoveredData(regionEntry);
                      }}
                      onMouseLeave={() => {
                        setHoveredName(null);
                        setHoveredData(null);
                      }}
                      style={{
                        default: {
                          fill,
                          stroke: isSelected ? "#1e40af" : "#cbd5e1",
                          strokeWidth: isSelected ? 1.8 : 0.7,
                          outline: "none",
                          transition: "all 200ms ease-out",
                        },
                        hover: {
                          fill: "#fbbf24",
                          stroke: "#f59e0b",
                          strokeWidth: 1.2,
                          cursor: "pointer",
                        },
                        pressed: {
                          fill: "#f59e0b",
                        },
                      }}
                    />
                  );
                })
              }
            </Geographies>
          </ZoomableGroup>
        </ComposableMap>

        {/* Tooltip on hover */}
        {hoveredData && (
          <div
            style={{
              position: "absolute",
              top: 12,
              left: 12,
              background: "rgba(15, 23, 42, 0.95)",
              color: "#fff",
              padding: "10px 14px",
              borderRadius: 8,
              fontSize: 13,
              pointerEvents: "none",
              zIndex: 50,
              boxShadow: "0 4px 12px rgba(0,0,0,0.2)",
            }}
          >
            <div style={{ fontWeight: 700, marginBottom: 4 }}>{hoveredName}</div>
            <div style={{ fontSize: 12, color: "#cbd5e1" }}>
              Deposits: {formatCurrency(hoveredData.deposits)}
            </div>
          </div>
        )}
      </div>

      {/* Legend */}
      <div
        style={{
          marginTop: 12,
          fontSize: 11,
          color: "#64748b",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span style={{ fontSize: 12 }}>
          {hoveredName ? `📍 ${hoveredName}` : "Scroll to zoom • Drag to pan"}
        </span>

        {maxVal > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span>Deposits:</span>
            <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span>Low</span>
              <div
                style={{
                  width: 80,
                  height: 8,
                  borderRadius: 4,
                  background: "linear-gradient(90deg, #a7f3d0 0%, #6ee7b7 33%, #3b82f6 66%, #1e40af 100%)",
                  border: "1px solid #e2e8f0",
                }}
              />
              <span>High</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}