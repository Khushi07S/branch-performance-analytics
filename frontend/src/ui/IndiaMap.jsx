// frontend/src/ui/IndiaMap.jsx
import React, { useEffect, useState, useMemo } from "react";
import { ComposableMap, Geographies, Geography } from "react-simple-maps";

/*
  This component tries to fetch topojson from two locations:
   1) frontend/src/ui/india.topo.json (imported at build time if present)
   2) /mnt/data/india.topo.json (dev environment file)

  Place a topojson at either path. If missing, a friendly message is shown.
*/

const DEFAULT_TOPO_URLS = [
  "/src/ui/india.topo.json", // relative (works when file is in frontend/src/ui)
  "/mnt/data/india.topo.json" // dev mount path used in this environment
];

function countryToColor(val, max) {
  if (!val) return "#f1f5f9";
  const intensity = Math.round((val / (max || 1)) * 180);
  return `rgb(${255 - intensity}, ${245 - Math.round(intensity / 2)}, ${220 - Math.round(intensity / 4)})`;
}

export default function IndiaMap({ regions = [], onRegionClick = () => {}, selectedRegion = null }) {
  const [topo, setTopo] = useState(null);
  const [loading, setLoading] = useState(true);
  const safeRegions = Array.isArray(regions) ? regions : [];

  useEffect(() => {
    let mounted = true;
    async function tryLoad() {
      setLoading(true);
      for (const url of DEFAULT_TOPO_URLS) {
        try {
          const resp = await fetch(url);
          if (!resp.ok) continue;
          const json = await resp.json();
          if (mounted) { setTopo(json); setLoading(false); return; }
        } catch (e) {
          // try next
        }
      }
      if (mounted) { setTopo(null); setLoading(false); }
    }
    tryLoad();
    return () => { mounted = false; };
  }, []);

  const maxVal = safeRegions.length ? Math.max(...safeRegions.map(r => r.deposits || 0)) : 1;
  const byName = useMemo(() => {
    const m = {};
    safeRegions.forEach(r => {
      const key = (r.region || r.state || "").toString().toLowerCase();
      if (key) m[key] = r;
    });
    return m;
  }, [safeRegions]);

  const [hovered, setHovered] = useState(null);

  if (loading) {
    return <div style={{ height: 240, display: "flex", alignItems: "center", justifyContent: "center", color: "#64748b" }}>Loading map…</div>;
  }

  if (!topo) {
    return <div style={{ height: 240, display: "flex", alignItems: "center", justifyContent: "center", color: "#64748b" }}>Map data not found — place india.topo.json at frontend/src/ui/ or /mnt/data/india.topo.json</div>;
  }

  return (
    <div style={{ width: "100%", height: 240 }}>
      <ComposableMap projection="geoMercator" projectionConfig={{ scale: 1000, center: [80, 22] }}>
        <Geographies geography={topo}>
          {({ geographies }) => geographies.map(geo => {
            const name = (geo.properties && (geo.properties.NAME_1 || geo.properties.name)) || geo.id || "unknown";
            const key = name.toString().toLowerCase();
            const entry = byName[key];
            const val = entry?.deposits ?? 0;
            const fill = countryToColor(val, maxVal);
            const isSelected = selectedRegion && selectedRegion.toLowerCase() === key;
            return (
              <Geography
                key={geo.rsmKey || geo.geojsonId || geo.id || name}
                geography={geo}
                onClick={() => onRegionClick(entry?.region ?? name)}
                onMouseEnter={() => setHovered(key)}
                onMouseLeave={() => setHovered(null)}
                style={{
                  default: { fill: fill, stroke: isSelected ? "#0f172a" : "#cbd5e1", strokeWidth: isSelected ? 1.6 : 0.6, outline: "none", transition: "all 150ms" },
                  hover: { fill: "#ffedd5", stroke: "#94a3b8", cursor: "pointer" },
                  pressed: { fill: "#ffd080" },
                }}
              />
            );
          })}
        </Geographies>
      </ComposableMap>
      <div style={{ marginTop: 6, fontSize: 12, color: "#64748b" }}>{hovered ? `Hovered: ${hovered}` : "Click a region to drill into branches"}</div>
    </div>
  );
}
