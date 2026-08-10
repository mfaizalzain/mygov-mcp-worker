/**
 * mygov-mcp Worker - Model Context Protocol (streamable HTTP) server exposing
 * Malaysia's Government Open API (api.data.gov.my) as read-only tools.
 *
 * Serves the same 6 tools as the bundled stdio plugin server, hosted so that
 * ChatGPT web / Codex can reach it (the OpenAI plugin portal scans this URL).
 *
 *   POST /mcp  - JSON-RPC over streamable HTTP (initialize, tools/list, tools/call)
 *   GET  /.well-known/openai-apps-challenge - domain verification token
 *
 * Rate limits: api.data.gov.my allows 4 req/min per API family. We keep a
 * per-isolate rolling throttle (same policy as the stdio server) and cache
 * slow-changing responses at the edge.
 */

const BASE = "https://api.data.gov.my";
const UA = "mygov-mcp/1.0 (+https://mygov-mcp.faizalmzain.com)";

/* ---- per-isolate rolling throttle: 4 requests / 60 s per family ---- */
const FAMILY_HITS = new Map(); // family -> [timestamps]
async function throttle(family) {
  const now = Date.now();
  const hits = (FAMILY_HITS.get(family) || []).filter(t => now - t < 60000);
  if (hits.length >= 4) {
    const wait = 60000 - (now - hits[0]) + 200;
    await new Promise(r => setTimeout(r, wait));
  }
  FAMILY_HITS.set(family, [...hits, Date.now()]);
}

/* ---- upstream fetch helpers ---- */
const json = (body, status = 200, extra = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "cache-control": "no-store",
      ...extra,
    },
  });

async function apiGet(path, params = {}, ttl = 0) {
  const url = new URL(BASE + path);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  }
  // Edge cache via cf options (same pattern as the dashboard Worker's FIDS
  // route): Cloudflare caches the upstream response keyed by URL. ttl=0 skips
  // caching entirely (live feeds). NOTE: cf.cacheTtl is ignored by wrangler
  // dev — local runs always hit upstream, which is fine for testing.
  const cf = ttl > 0 ? { cacheTtl: ttl, cacheEverything: true } : undefined;
  const res = await fetch(url, { headers: { "user-agent": UA }, cf });
  if (!res.ok) throw new Error(`upstream ${res.status}`);
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("json")) return res.json();
  return new Uint8Array(await res.arrayBuffer()); // binary (zip / protobuf)
}

/* ---- minimal GTFS-realtime protobuf wire parser (subset we need) ---- */
function readVarint(buf, pos) {
  let result = 0, shift = 0;
  while (true) {
    const b = buf[pos++];
    result |= (b & 0x7f) << shift;
    if (!(b & 0x80)) return [result, pos];
    shift += 7;
  }
}

function parsePosition(buf) {
  const p = {};
  let pos = 0;
  const n = buf.length;
  while (pos < n) {
    const [tag, npos] = readVarint(buf, pos); pos = npos;
    const field = tag >> 3, wire = tag & 7;
    if (wire === 5) { // float32
      const val = buf.getFloat32 ? buf.getFloat32(pos, true)
        : new DataView(buf.buffer, buf.byteOffset + pos, 4).getFloat32(0, true);
      pos += 4;
      if (field === 1) p.lat = Math.round(val * 1e6) / 1e6;
      else if (field === 2) p.lon = Math.round(val * 1e6) / 1e6;
      else if (field === 3) p.bearing = Math.round(val * 10) / 10;
      else if (field === 5) p.speed = Math.round(val * 10) / 10;
    } else if (wire === 0) { const [, p2] = readVarint(buf, pos); pos = p2; }
    else if (wire === 2) { const [ln, p2] = readVarint(buf, pos); pos = p2 + ln; }
    else if (wire === 1) pos += 8;
    else break;
  }
  return p;
}

function parseVehiclePosition(buf) {
  const vp = {};
  let pos = 0;
  const n = buf.length;
  while (pos < n) {
    const [tag, npos] = readVarint(buf, pos); pos = npos;
    const field = tag >> 3, wire = tag & 7;
    if (wire === 2) {
      const [ln, p2] = readVarint(buf, pos); pos = p2;
      if (field === 2) { // position
        const p = parsePosition(buf.subarray(p2, p2 + ln));
        if (p.lat != null) vp.lat = p.lat;
        if (p.lon != null) vp.lon = p.lon;
        if (p.bearing != null) vp.bearing = p.bearing;
        if (p.speed != null) vp.speed = p.speed;
      }
      pos = p2 + ln;
    } else if (wire === 0) {
      const [val, p2] = readVarint(buf, pos); pos = p2;
      if (field === 6) vp.timestamp = val;
    } else if (wire === 5) pos += 4;
    else if (wire === 1) pos += 8;
    else break;
  }
  return vp;
}

function parseFeedEntity(buf) {
  const ent = { id: null };
  let pos = 0;
  const n = buf.length;
  while (pos < n) {
    const [tag, npos] = readVarint(buf, pos); pos = npos;
    const field = tag >> 3, wire = tag & 7;
    if (wire === 2) {
      const [ln, p2] = readVarint(buf, pos); pos = p2;
      if (field === 1) ent.id = new TextDecoder().decode(buf.subarray(p2, p2 + ln));
      else if (field === 8) { // vehicle -> VehiclePosition
        const vp = parseVehiclePosition(buf.subarray(p2, p2 + ln));
        if (vp.lat != null) ent.lat = vp.lat;
        if (vp.lon != null) ent.lon = vp.lon;
        if (vp.bearing != null) ent.bearing = vp.bearing;
        if (vp.speed != null) ent.speed = vp.speed;
        if (vp.timestamp != null) ent.timestamp = vp.timestamp;
      }
      pos = p2 + ln;
    } else if (wire === 0) { const [, p2] = readVarint(buf, pos); pos = p2; }
    else if (wire === 5) pos += 4;
    else if (wire === 1) pos += 8;
    else break;
  }
  return ent;
}

function parseFeedMessage(data) {
  const vehicles = [];
  let pos = 0;
  const n = data.length;
  while (pos < n) {
    const [tag, npos] = readVarint(data, pos); pos = npos;
    const field = tag >> 3, wire = tag & 7;
    if (wire === 2) {
      const [ln, p2] = readVarint(data, pos); pos = p2;
      if (field === 2) { // entity
        const ent = parseFeedEntity(data.subarray(p2, p2 + ln));
        if (ent.id != null) vehicles.push(ent);
      }
      pos = p2 + ln;
    } else if (wire === 0) { const [, p2] = readVarint(data, pos); pos = p2; }
    else if (wire === 5) pos += 4;
    else if (wire === 1) pos += 8;
    else break;
  }
  return vehicles;
}

/* ---- minimal ZIP reader (GTFS static archives: deflate entries) ---- */
function zipReadEntry(buf, name) {
  // find End of Central Directory
  let eocd = -1;
  for (let i = buf.length - 22; i >= 0; i--) {
    if (buf[i] === 0x50 && buf[i + 1] === 0x4b && buf[i + 2] === 0x05 && buf[i + 3] === 0x06) { eocd = i; break; }
  }
  if (eocd < 0) return null;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.length);
  const nEntries = dv.getUint16(eocd + 10, true);
  let off = dv.getUint32(eocd + 16, true);
  for (let i = 0; i < nEntries; i++) {
    if (dv.getUint32(off, true) !== 0x02014b50) break; // central dir signature
    const method = dv.getUint16(off + 10, true);
    const compSize = dv.getUint32(off + 20, true);
    const nameLen = dv.getUint16(off + 28, true);
    const extraLen = dv.getUint16(off + 30, true);
    const commentLen = dv.getUint16(off + 32, true);
    const localOff = dv.getUint32(off + 42, true);
    const entryName = new TextDecoder().decode(buf.subarray(off + 46, off + 46 + nameLen));
    if (entryName === name) {
      const lh = new DataView(buf.buffer, buf.byteOffset, buf.length);
      const ln = lh.getUint16(localOff + 26, true);
      const el = lh.getUint16(localOff + 28, true);
      const dataStart = localOff + 30 + ln + el;
      const comp = buf.subarray(dataStart, dataStart + compSize);
      if (method === 0) return new TextDecoder().decode(comp); // stored
      // method 8 = deflate; ZIP stores raw DEFLATE (no zlib wrapper), and
      // Workers' DecompressionStream needs a stream source to pipe.
      const ds = new DecompressionStream("deflate-raw");
      const stream = new Blob([comp]).stream().pipeThrough(ds);
      return new Response(stream).arrayBuffer()
        .then(ab => new TextDecoder().decode(ab));
    }
    off += 46 + nameLen + extraLen + commentLen;
  }
  return null;
}

async function gtfsStaticSummary(agency, ttl = 0) {
  if (!/^[a-z0-9-]{1,32}$/.test(agency)) throw new Error("invalid agency");
  const category = agency.startsWith("prasarana") ? "rapid-bus-kl" : null;
  const data = await apiGet(`/gtfs-static/${agency}`, category ? { category } : {}, ttl);
  const summary = { agency, files: [] };
  const dv = new DataView(data.buffer, data.byteOffset, data.length);
  let eocd = -1;
  for (let i = data.length - 22; i >= 0; i--) {
    if (data[i] === 0x50 && data[i + 1] === 0x4b && data[i + 2] === 0x05 && data[i + 3] === 0x06) { eocd = i; break; }
  }
  if (eocd < 0) return { ...summary, error: "not a zip" };
  const nEntries = dv.getUint16(eocd + 10, true);
  let off = dv.getUint32(eocd + 16, true);
  const fileList = [];
  for (let i = 0; i < nEntries; i++) {
    const nameLen = dv.getUint16(off + 28, true);
    const extraLen = dv.getUint16(off + 30, true);
    const commentLen = dv.getUint16(off + 32, true);
    fileList.push(new TextDecoder().decode(data.subarray(off + 46, off + 46 + nameLen)));
    off += 46 + nameLen + extraLen + commentLen;
  }
  summary.files = fileList;
  for (const fname of ["routes.txt", "stops.txt", "trips.txt"]) {
    const txt = await zipReadEntry(data, fname);
    if (txt == null) continue;
    const lines = txt.split(/\r?\n/).filter(l => l.length);
    if (!lines.length) continue;
    const header = lines[0].split(",");
    summary[`${fname.replace(".txt", "")}_rows`] = lines.length - 1;
    if (fname === "routes.txt" && lines.length > 1) {
      summary.sample_routes = lines.slice(1, 6).map(l =>
        Object.fromEntries(header.map((h, i) => [h, l.split(",")[i]])));
    }
  }
  return summary;
}

/* ---- Rapid KL live bus feed (myrapidbus kiosk data source) ----
 * The api.data.gov.my GTFS-RT feed for prasarana is frequently empty, but the
 * official kiosk (myrapidbus.prasarana.com.my/kiosk) shows live buses from a
 * socket.io server (rapidbus-socketio-avl.prasarana.com.my). socket.io's
 * engine.io polling transport is plain HTTP, so a Worker can consume it:
 *   1. GET  /socket.io/?EIO=4&transport=polling   -> 0{"sid":...}
 *   2. POST 40{...} connect, POST 42["onFts-reload",{...}] emit
 *   3. GET  poll -> 42["onFts-client","<base64(gzip(json))>"]
 * Data shape per bus: bus_no, latitude, longitude, route, dir, speed, angle,
 * dt_gps, dt_received, captain_id, trip_no, engine_status, accessibility.
 */
const RAPID_SID = "m0ckulfr515l5s79sgd2hhva9iqm3cr2"; // shared kiosk sid
const RAPID_URL = "https://rapidbus-socketio-avl.prasarana.com.my/socket.io/";

function b64decode(s) {
  const bin = atob(s);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

async function rapidBusFeed(provider = "RKL", route = "") {
  const t = Date.now();
  // 1. open
  const openRes = await fetch(`${RAPID_URL}?EIO=4&transport=polling&t=${t}`, {
    headers: { "user-agent": UA },
  });
  if (!openRes.ok) throw new Error(`rapid open ${openRes.status}`);
  const openText = await openRes.text();
  const m = openText.match(/^0\{"sid":"([^"]+)"/);
  if (!m) throw new Error(`rapid open handshake failed: ${openText.slice(0, 80)}`);
  const sid = m[1];
  // 2. connect + emit (two POSTs, same sid)
  const post = async (payload) => {
    const r = await fetch(`${RAPID_URL}?EIO=4&transport=polling&sid=${sid}&t=${Date.now()}`, {
      method: "POST",
      headers: { "content-type": "text/plain;charset=UTF-8", "user-agent": UA },
      body: payload,
    });
    if (!r.ok) throw new Error(`rapid post ${r.status}`);
  };
  await post(`40{"sid":"${RAPID_SID}","uid":""}`);
  await post(`42["onFts-reload",{"sid":"${RAPID_SID}","uid":"","provider":"${provider}","route":"${route}"}]`);
  // 3. poll for the response
  await new Promise(r => setTimeout(r, 1500));
  const pollRes = await fetch(`${RAPID_URL}?EIO=4&transport=polling&sid=${sid}&t=${Date.now()}`, {
    headers: { "user-agent": UA },
  });
  if (!pollRes.ok) throw new Error(`rapid poll ${pollRes.status}`);
  const pollText = await pollRes.text();
  // frames split by \x1e; find the 42["onFts-client",...] frame
  let payload = null;
  for (const frame of pollText.split("\x1e")) {
    const fm = frame.match(/^42\["onFts-client","(.*)"\]$/, "s");
    if (fm) { payload = fm[1]; break; }
  }
  if (!payload) throw new Error(`no onFts-client frame in poll (${pollText.slice(0, 60)})`);
  // payload is base64(gzip(json)) — decompress in the Worker
  const ds = new DecompressionStream("gzip");
  const stream = new Blob([b64decode(payload)]).stream().pipeThrough(ds);
  const text = new TextDecoder().decode(await new Response(stream).arrayBuffer());
  return JSON.parse(text);
}

async function rapidBusLive(provider, route) {
  const data = await rapidBusFeed(provider, route || "");
  const buses = Array.isArray(data) ? data : [];
  return {
    provider,
    route: route || "all",
    live_buses: buses.length,
    updated: new Date().toISOString(),
    buses: buses.slice(0, 200).map(b => ({
      bus_no: b.bus_no, latitude: b.latitude, longitude: b.longitude,
      route: b.route, dir: b.dir, speed: b.speed, angle: b.angle,
      dt_gps: b.dt_gps, trip_no: b.trip_no, accessibility: b.accessibility,
    })),
  };
}

/* ---- MCP tool catalogue (mirrors the stdio plugin server) ---- */
const RO = { readOnlyHint: true, openWorldHint: false, destructiveHint: false };
const TOOLS = [
  {
    name: "mygov_weather_forecast",
    description: "7-day weather forecast for Malaysia locations (MET Malaysia). "
      + "Optionally filter by location name (e.g. 'Kota Bharu', 'Langkawi'). "
      + "Returns date, morning/afternoon/night forecast, min/max temp.",
    inputSchema: {
      type: "object",
      properties: {
        location: { type: "string", description: "Optional location name filter" },
        limit: { type: "integer", description: "Max records (default 200)" },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_weather_warning",
    description: "Active weather warnings for Malaysia (MET Malaysia).",
    inputSchema: { type: "object", properties: {} },
    annotations: RO,
  },
  {
    name: "mygov_data_catalogue",
    description: "Query the Data Catalogue API (general gov datasets). Known ids: "
      + "fuelprice (RON95/RON97/diesel weekly). Filters use value@column syntax, "
      + "e.g. {'filter': 'level@series_type'} or {'sort': '-date'}.",
    inputSchema: {
      type: "object",
      properties: {
        dataset_id: { type: "string", description: "Dataset id, e.g. fuelprice" },
        limit: { type: "integer", description: "Max records (default 100)" },
        filter: { type: "string", description: "value@column exact match" },
        contains: { type: "string", description: "value@column partial match" },
        sort: { type: "string", description: "column or -column" },
        date_start: { type: "string", description: "YYYY-MM-DD@date" },
        date_end: { type: "string", description: "YYYY-MM-DD@date" },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_opendosm",
    description: "Query the OpenDOSM API (DOSM economics/statistics). Known ids: "
      + "cpi_core (CPI index). Supports same filters as data catalogue.",
    inputSchema: {
      type: "object",
      properties: {
        dataset_id: { type: "string", description: "Dataset id, e.g. cpi_core" },
        limit: { type: "integer", description: "Max records (default 100)" },
        filter: { type: "string", description: "value@column exact match" },
        sort: { type: "string", description: "column or -column" },
        date_start: { type: "string", description: "YYYY-MM-DD@date" },
        date_end: { type: "string", description: "YYYY-MM-DD@date" },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_gtfs_static_summary",
    description: "Download a GTFS static schedule ZIP (ktmb, prasarana, mybas-kota-bharu, "
      + "mybas-alor-setar, mybas-kuala-terengganu, mybas-johor-bahru, etc.) and "
      + "return file list, row counts, and sample routes.",
    inputSchema: {
      type: "object",
      properties: {
        agency: { type: "string", description: "e.g. ktmb, prasarana, mybas-kota-bharu" },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_gtfs_realtime",
    description: "Live vehicle positions (GTFS-realtime protobuf). Agencies: ktmb, "
      + "prasarana (category rapid-bus-kl / rapid-rail-kl), mybas-*. "
      + "Returns count + vehicle id/lat/lon list. NOTE: the prasarana GTFS-RT feed "
      + "is often empty — use mygov_rapid_bus_live for actual Rapid KL bus positions.",
    inputSchema: {
      type: "object",
      properties: {
        agency: { type: "string", description: "ktmb, prasarana, or mybas-*" },
        category: { type: "string", description: "For prasarana: rapid-bus-kl, rapid-rail-kl, ..." },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_rapid_bus_live",
    description: "Live Rapid KL / Rapid Penang / Rapid Kuantan bus positions from the "
      + "official myrapidbus kiosk feed (800+ buses). Providers: RKL (Klang Valley), "
      + "RPG (Penang), RKN (Kuantan). Optional route filter (e.g. T2000, 300). "
      + "Returns bus_no, lat/lon, route, speed, direction, last GPS time.",
    inputSchema: {
      type: "object",
      properties: {
        provider: { type: "string", description: "RKL, RPG, or RKN (default RKL)" },
        route: { type: "string", description: "Optional route number filter" },
      },
    },
    annotations: RO,
  },
  {
    name: "mygov_flood_risk",
    description: "Live flood risk from JPS (Department of Irrigation and Drainage) "
      + "water-level telemetry. Returns stations currently at danger/warning/alert "
      + "(only gauges that reported within the last 24h - dead gauges excluded), "
      + "each with name, state, district, lat/lon, water level, danger threshold, "
      + "trend and last reading time, plus a per-state count summary.",
    inputSchema: {
      type: "object",
      properties: {},
    },
    annotations: RO,
  },
];

/* ---- tool dispatch ---- */
/* TTLs: how long a response may sit in the edge cache. Slow-moving government
   data (fuel weekly, CPI quarterly) gets long TTLs so the upstream API sees a
   handful of requests per day regardless of how many agents query. Live feeds
   (gtfs-realtime) are never cached. */
const TTL = {
  "mygov_weather_forecast": 1800,      // MET Malaysia: updates through the day
  "mygov_weather_warning": 600,        // warnings can change quickly
  "mygov_data_catalogue": 21600,       // fuelprice weekly, exchangerates daily
  "mygov_opendosm": 43200,             // CPI quarterly, IPI monthly
  "mygov_gtfs_static_summary": 43200,  // schedules republished daily
  "mygov_gtfs_realtime": 0,            // live positions — never cached
  "mygov_rapid_bus_live": 0,           // live kiosk feed — never cached
  "mygov_flood_risk": 300,             // JPS telemetry updates every 15 min
};

async function callTool(name, args) {
  const a = args || {};
  const ttl = TTL[name] || 0;
  /* Arbitrary client-supplied limits must not be able to force a giant
     upstream fetch (and its cached copy). Clamp to a sane maximum. */
  const clampLimit = v => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.min(Math.max(Math.floor(n), 1), 1000) : 100;
  };
  if (name === "mygov_weather_forecast") {
    await throttle("weather");
    const params = { limit: clampLimit(a.limit || 200) };
    if (a.location) params.contains = `${a.location}@location__location_name`;
    return apiGet("/weather/forecast", params, ttl);
  }
  if (name === "mygov_weather_warning") {
    await throttle("weather");
    return apiGet("/weather/warning", {}, ttl);
  }
  if (name === "mygov_data_catalogue") {
    await throttle("data-catalogue");
    const params = { id: a.dataset_id || "", limit: clampLimit(a.limit || 100) };
    for (const k of ["filter", "contains", "sort", "date_start", "date_end"]) {
      if (a[k]) params[k] = a[k];
    }
    return apiGet("/data-catalogue", params, ttl);
  }
  if (name === "mygov_opendosm") {
    await throttle("opendosm");
    const params = { id: a.dataset_id || "", limit: clampLimit(a.limit || 100) };
    for (const k of ["filter", "sort", "date_start", "date_end"]) {
      if (a[k]) params[k] = a[k];
    }
    return apiGet("/opendosm", params, ttl);
  }
  if (name === "mygov_gtfs_static_summary") {
    await throttle("gtfs");
    return gtfsStaticSummary(a.agency || "ktmb", ttl);
  }
  if (name === "mygov_gtfs_realtime") {
    await throttle("gtfs");
    const agency = String(a.agency || "ktmb");
    if (!/^[a-z0-9-]{1,32}$/.test(agency)) throw new Error("invalid agency");
    const category = a.category ? String(a.category) : "";
    if (category && !/^[a-z0-9-]{1,32}$/.test(category)) throw new Error("invalid category");
    const path = `/gtfs-realtime/vehicle-position/${agency}`;
    const data = await apiGet(path, category ? { category } : {}, 0);
    const vehicles = parseFeedMessage(data);
    return { agency, live_vehicles: vehicles.length, vehicles: vehicles.slice(0, 100) };
  }
  if (name === "mygov_rapid_bus_live") {
    const provider = String(a.provider || "RKL").toUpperCase();
    if (!["RKL", "RPG", "RKN"].includes(provider)) {
      throw new Error(`unknown provider ${provider} (use RKL, RPG, or RKN)`);
    }
    // route is interpolated into the kiosk's engine.io emit payload - keep it
    // to the alphanumeric-dash shape a real route id has.
    const route = String(a.route || "");
    if (route && !/^[A-Za-z0-9-]{1,16}$/.test(route)) throw new Error("invalid route");
    return rapidBusLive(provider, route);
  }
  if (name === "mygov_flood_risk") {
    // Proxy the dashboard's /api/flood route: same JPS feed, same slimming
    // (danger/warning/alert + 24h freshness), so agents get the identical
    // picture the dashboard shows.
    const FEED = "https://mygov.faizalmzain.com/api/flood";
    let res;
    try {
      res = await fetch(FEED, { headers: { "user-agent": UA }, cf: { cacheTtl: ttl } });
    } catch {
      throw new Error("flood upstream unreachable");
    }
    if (!res.ok) throw new Error(`flood upstream error ${res.status}`);
    const data = await res.json();
    return {
      updated: data.updated,
      at_risk: data.at_risk,
      states: data.states,
      stations: data.stations,
    };
  }
  throw new Error(`Unknown tool: ${name}`);
}

/* ---- MCP JSON-RPC plumbing ---- */
const rpcResult = (id, result) => ({ jsonrpc: "2.0", id, result });
const rpcError = (id, code, message) => ({ jsonrpc: "2.0", id, error: { code, message } });

async function handleMCPMessage(msg) {
  const method = msg.method;
  const id = msg.id !== undefined ? msg.id : null;
  if (method === "initialize") {
    return rpcResult(id, {
      protocolVersion: "2024-11-05",
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: "mygov-api-mcp", version: "1.0.0" },
    });
  }
  if (method === "notifications/initialized" || method === "ping") return null;
  if (method === "tools/list") {
    return rpcResult(id, { tools: TOOLS });
  }
  if (method === "tools/call") {
    const params = msg.params || {};
    try {
      const result = await callTool(params.name, params.arguments || {});
      return rpcResult(id, {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        isError: false,
      });
    } catch (e) {
      return rpcError(id, -32000, `${e.constructor.name}: ${e.message}`);
    }
  }
  return rpcError(id, -32601, `Method not found: ${method}`);
}

/* ---- HTTP entry ---- */
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get("origin") || "*";

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": origin,
          "access-control-allow-methods": "POST, GET, OPTIONS",
          "access-control-allow-headers": "content-type, mcp-session-id, authorization",
          "access-control-max-age": "86400",
        },
      });
    }

    // OpenAI domain verification (token is set as a secret: OPENAI_CHALLENGE_TOKEN)
    if (url.pathname === "/.well-known/openai-apps-challenge") {
      const token = env.OPENAI_CHALLENGE_TOKEN || "";
      if (!token) return new Response("not configured", { status: 404 });
      return new Response(token, {
        headers: { "content-type": "text/plain; charset=utf-8", "cache-control": "no-store" },
      });
    }

    if (url.pathname !== "/mcp") {
      return json({ error: "not_found", hint: "POST /mcp for MCP; /.well-known/openai-apps-challenge for domain verification" }, 404);
    }
    if (request.method !== "POST") {
      return json({ error: "method_not_allowed", hint: "MCP streamable HTTP uses POST" }, 405);
    }

    let msg;
    try { msg = await request.json(); }
    catch { return json({ error: "invalid_json" }, 400); }

    const out = await handleMCPMessage(msg);
    if (out === null) return new Response(null, { status: 202, headers: { "access-control-allow-origin": origin } });
    return json(out, 200, { "access-control-allow-origin": origin });
  },
};


