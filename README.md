# mygov-mcp Worker

Streamable-HTTP MCP server exposing Malaysia's Government Open API
([api.data.gov.my](https://developer.data.gov.my)) and the dashboard's
collected datasets as read-only tools, hosted at:

- **https://mygov-mcp.faizalmzain.com/mcp** (primary)
- **https://mcp.malaysia-at-a-glance.com/mcp** (second custom domain — both URLs
  serve the identical Worker; CORS echoes any origin, the OpenAI challenge
  token is env-based, so nothing host-specific lives in the code)

Serves the same 14 tools as the bundled plugin server
([mfaizalzain/mygov-mcp](https://github.com/mfaizalzain/mygov-mcp)) so ChatGPT
Work / Codex can reach them over HTTPS — this is the URL the OpenAI plugin
submission portal scans.

## Endpoints

- `POST /mcp` - MCP streamable-HTTP JSON-RPC (initialize, tools/list, tools/call)
- `GET /.well-known/openai-apps-challenge` - OpenAI domain verification
  (serves the token from the `OPENAI_CHALLENGE_TOKEN` secret; 404 until set)

## Tools (17, all read-only, `readOnlyHint: true`)

mygov_weather_forecast, mygov_weather_warning, mygov_data_catalogue,
mygov_opendosm, mygov_gtfs_static_summary, mygov_gtfs_realtime,
mygov_rapid_bus_live, mygov_flood_risk, mygov_pricecatcher,
mygov_tourism_arrivals, mygov_rapid_service_alert, mygov_air_quality,
mygov_hotel_performance (quarterly hotel occupancy/room rate/guests by state,
Tourism Malaysia), mygov_election_results (latest SPR election results:
PRU-15, state elections for all 13 states, latest by-election),
mygov_search (find datasets across the data.gov.my + OpenDOSM catalogues by
topic — 470+ datasets, ranked by id/title/category/description match),
mygov_health (server + upstream status; `probe=true` tests every source with
per-source latency), mygov_dataset_info (publisher metadata for one dataset:
last update, next update due, columns, latest row).

The dashboard-backed tools proxy the dashboard's collected static files
(`rapid_alerts.json`, `/api/aqi`, `hotel.json`, `election.json`) with the same
edge-cache TTLs the live tools use — agents see exactly what the dashboard
shows. The catalogue tools (`mygov_search`, `mygov_dataset_info`) parse the
portals' `__NEXT_DATA__` blobs, the same way the stdio plugin server does.

## Local development

```bash
npx wrangler dev --port 8789
```

Then drive it with any MCP client. Quick smoke test:

```bash
curl -s -X POST http://127.0.0.1:8789/mcp \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
curl -s -X POST http://127.0.0.1:8789/mcp \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
```

## Deploy

```bash
npx wrangler deploy   # custom domains: mygov-mcp.faizalmzain.com + mcp.malaysia-at-a-glance.com
```

Set the OpenAI challenge token (when the submission portal provides one):

```bash
npx wrangler secret put OPENAI_CHALLENGE_TOKEN
```

## Notes

- Rate limit: api.data.gov.my allows 4 req/min per API family; the Worker keeps
  a per-isolate rolling throttle and never exceeds it.
- GTFS static ZIPs are parsed in-Worker with a minimal deflate reader; GTFS-RT
  protobuf is parsed with a hand-rolled wire parser (no dependencies, matching
  the stdio server's stdlib-only design).
- CORS is open so browser-based MCP clients can call it directly.
- Keep the tool catalogue byte-consistent with the stdio plugin server
  (`mygov-mcp/codex-mygov/servers/server.py` + `claude-mygov/servers/server.py`)
  — the Worker mirrors it in JS.
