# mygov-mcp Worker

Streamable-HTTP MCP server exposing Malaysia's Government Open API
([api.data.gov.my](https://developer.data.gov.my)) as read-only tools, hosted at
**https://mygov-mcp.faizalmzain.com/mcp**.

Serves the same 6 tools as the bundled plugin server
([mfaizalzain/mygov-mcp](https://github.com/mfaizalzain/mygov-mcp)) so ChatGPT
Work / Codex can reach them over HTTPS - this is the URL the OpenAI plugin
submission portal scans.

## Endpoints

- `POST /mcp` - MCP streamable-HTTP JSON-RPC (initialize, tools/list, tools/call)
- `GET /.well-known/openai-apps-challenge` - OpenAI domain verification
  (serves the token from the `OPENAI_CHALLENGE_TOKEN` secret; 404 until set)

## Tools (all read-only, `readOnlyHint: true`)

mygov_weather_forecast, mygov_weather_warning, mygov_data_catalogue,
mygov_opendosm, mygov_gtfs_static_summary, mygov_gtfs_realtime.

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
npx wrangler deploy   # creates the mygov-mcp.faizalmzain.com custom domain
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
