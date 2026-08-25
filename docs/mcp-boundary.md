# Hermes G2 MCP boundary

This document is the release contract for the glasses integration. Workflow
authority must not live in a profile `SOUL.md`, a model prompt, or a skill.
Descriptions may help the model choose a tool, but authentication, routing,
validation, retries, consent, idempotency, and delivery receipts are code.

## Two MCP roles

The authenticated phone WebSocket multiplexes two independent MCP sessions:

- **Host Session MCP** (`host-mcp`): the phone is the client and Hermes is the
  server. It owns voice-turn submission/cancellation and the bounded read-only
  Cockpit connection/status resource. When both peers explicitly negotiate
  `conversate-cues-v1`, it also owns the bounded `hermes.conversate.cues`
  auxiliary side lane. Companion is deliberately unavailable.
- **G2 Device MCP** (`mcp`): Hermes is the client and the phone is the server.
  It owns phone/glasses tools. A call is authorized by connection-bound host
  state; model arguments never grant a turn, profile, device, or proactive
  capability.

The remaining `ctl` channel is transport-only authentication, version
negotiation, keepalive, and capability discovery. A client that negotiates
`host-mcp-v1` must complete `initialize` and `notifications/initialized` within
the bounded connection deadline. A client that does not advertise that
capability is disconnected; there is no release fallback. Custom `chat`,
`cockpit`, and `companion` frames are inert for every authenticated client.

## Workflow package

`hermes-g2-workflows` is a portable Agent Plugin MCP server. It exposes only
intent-complete tools. Hermes injects a signed, package-, workflow-, argument-,
and turn-bound capability in MCP request `_meta`; the model cannot supply or
override it. Consent is an object grant for exact binding
`hermes-g2-workflows:workflows` plus the canonical package content digest;
legacy name-only grants fail closed. The capability uses the stable audience
`com.hermes.mcp/portable/hermes-g2-workflows/workflows` and is minted once per
logical call so an automatic MCP reconnect reuses the identical nonce.

The server reaches the native bridge through a private, same-UID Unix relay.
Its `0600` socket and `SO_PEERCRED` check are transport isolation, not authority,
and no readable token file exists. The native process verifies the HMAC,
audience, binding, package digest, workflow, original argument digest, expiry,
nonce use, exact active turn, and reviewed subcall sequence before dispatching
through a fixed name-to-handler table, not a generic Python, phone-tool, or
Hermes registry call. Standard MCP cancellation closes the Unix request,
cancels native dispatch, and sends `notifications/cancelled` for the exact
in-flight phone MCP request; any late result is inert.

Capability packages are rejected at discovery and again at every mint if a
`__pycache__`, `.pyc`, or `.pyo` is present. The Python server starts with
`-I -S -B` and bytecode writes disabled, preventing ambient site hooks and
post-approval executable cache creation. Hermes rewrites its portable bare
`python` command to the host's resolved absolute interpreter and requires the
executed script to be a regular in-package file. Other capability runtimes may
only execute digest-included `./...` package files; bare ambient commands are
rejected.

| Interaction | Release owner | Current migration state |
|---|---|---|
| Voice turn, final result, cancellation | Host Session MCP | Active; no custom-channel fallback |
| Opt-in Conversate question/topic cues | Host Session MCP → tool-free auxiliary model | Optional negotiated capability; bounded recent text including live revisions, latest-wins, 2.5 s deadline, local fallback |
| Work Tasks add | Workflow MCP → fixed device MCP tool | Active |
| Hermes Kanban card create | Workflow MCP → fixed canonical Kanban DB call | Active; exact existing board only, blocked + unassigned, duplicate-proof retry |
| Clock timer/alarm set | Workflow MCP → fixed device MCP tool | Active |
| UK train departures + final deck | Workflow MCP → typed reader + fixed present | Active |
| UK public weather + final deck | Workflow MCP → typed reader + fixed present | Active |
| Apps, windows, launcher folders | Workflow MCP → pinned fixed device routes | Active |
| Media status and control | Workflow MCP → pinned fixed device routes | Active |
| Navigation start/stop/status | Workflow MCP → pinned fixed device routes | Active |
| Notification list/dismiss | Workflow MCP → pinned fixed device routes | Active |
| Coarse ring-health summary | Workflow MCP → consent-gated fixed read | Active |
| Phone calendar agenda | Workflow MCP → bounded fixed read | Active |
| Completed background result | Deterministic native producer → durable phone queue | Internal-only; not a model tool |
| One-shot reminder creation and fire | Workflow MCP → deterministic native outbox → fixed device MCP notify | Active |
| Context deck presentation | Intent-specific workflows → fixed server-authored deck | Weather/train only; generic present/pins deliberately not exposed |
| Cockpit current/recent G2 sessions and reviewed commands | Host Session MCP state resource + exact command tool | Active; listed-choice answer, deny/allow-once, steer, and interrupt only |
| Companion | None | Deliberately unavailable; legacy custom frames are inert |
| Home Assistant | Separate user-configured MCP | Remove personal aliases/policy from release profile |
| Calendar / printers | Separate user-configured MCPs | Remove recipes from release profile |
| Email watches and private host scripts | Optional local integration package | Not part of the public base package |
| Browser harness | Separate least-privilege MCP | Not part of the device authority boundary |

The Kanban workflow exposes one create intent, not the raw Kanban toolset. The
model must supply an exact existing board slug or display name. Zero or multiple
matches return typed `board_not_found` or `board_ambiguous` output with at most
16 canonical active board choices, and no card is written. There is no fallback
to local Work Tasks and no implicit match against Kanban statuses or lanes.
The exact active turn and cancellation state are revalidated after enumeration
and immediately before any private board display names can leave the host.
Successful cards are `blocked` and unassigned, not `triage`, because Hermes'
default triage auto-decomposer can otherwise start model work without another
wearer action. The create transaction records the canonical sticky-block event
as well as the initial status, preventing dependency recomputation from
promoting a parentless blocked card.

Bridge manifest 2.1.0 enforces cross-board idempotency with an owner-only,
profile-scoped operation ledger rather than relying on the race-tolerant
per-board lookup alone. The ledger stores a canonical payload digest, never a
second plaintext copy of title/body. Under a bounded POSIX `flock`, it commits
`PREPARED`, pins the exact slug plus original DB/directory/metadata generation,
and durably advances to `MUTATING` before any possible board write. The pinned
DB is opened in existing-file-only mode. Its generation and active metadata are
revalidated after open and inside `kanban_db.write_txn`; the exact live-turn
authority and cancellation flag are checked immediately before mutation and
again before that transaction can commit. Blocking lock and SQLite work runs
in a worker thread, keeping cancellation/revocation processing live on the
gateway event loop, and uses short bounded wait times.

After canonical create commits, the ledger records a permanent `COMMITTED`
tombstone containing only the stable task ID, canonical slug, and immutable
creation facts. Retries do not resolve a display name again and cannot target a
renamed or recreated board. Assignment, status changes, task archive/hard
delete, board archive/delete, and display-name or slug reuse cannot cause a new
card. Historical receipts say `created_status: blocked` and
`created_assignee: null`; they make no claim about current card state. A
`MUTATING` crash entry recovers only one exact, still-verifiable canonical
idempotency row. If no such row exists, including create-then-hard-delete in the
cross-DB crash window, the outcome remains permanently unknown and the workflow
will not recreate. This is an intentional fail-closed availability tradeoff.
Ledger directory/file modes are `0700`/`0600`, symlinks and hardlinks are
rejected, and non-POSIX hosts without secure `flock`/`O_NOFOLLOW` fail closed.
The canonical default board is the one exception to the existing-file rule:
Hermes lists it before its legacy DB exists, so the bridge may invoke canonical
default initialization under current authority and the global lock, before
`PREPARED` exists. Missing named generations are never initialized. A global
lock timeout is an exact unknown outcome, not `not_committed` or an authority
error, because a response-loss attempt may still hold the lock while committing
or finalizing the same identity.

## Browser Harness limited-release gap

The G2 profile does not currently enable a general Browser Harness MCP. Train
and weather requests are covered by their fixed typed Playwright/Brave readers,
but an unrelated public-web question must fail explicitly rather than opening a
visible browser or falling back to terminal automation. This remains a limited-
release gap.

An undeployed review candidate now lives in the sibling `hermes-public-web`
package. It deliberately requires a caller-supplied public HTTPS source URL and
returns one question-focused typed source record. It does not scrape a consumer
search UI: automated Brave search requires the official authenticated API, so
query-only discovery remains a separate gap. The candidate is not part of the
live G2 platform until its independent review and activation gates pass.

The smallest acceptable follow-on is a separate `hermes-public-web` MCP with no
G2 session capability and one query-level, read-only public-research tool. It
must run locked Playwright plus the reviewed Brave ELF in a dedicated process,
with an ephemeral `0700` profile, no personal cookies or extensions, no uploads
or downloads, no service workers or websockets, no credential store, bounded
concurrency/deadlines/output, and cancellation that closes the browser. Every
HTTPS navigation and redirect must re-resolve DNS and reject loopback, private,
link-local, metadata, and non-HTTP destinations. Results must be bounded typed
source records treated as untrusted data; the model must not receive a raw
page/DOM/browser-control/JavaScript/Python primitive.

The candidate's process-group cleanup and per-worker limits are not a complete
resource boundary: a live activation also needs a dedicated container or
cgroup with aggregate memory, task, CPU, temporary-storage, and kill controls.
Its typed `untrusted` result label is likewise not an authority boundary. The
host must require a fresh user-origin URL or explicit confirmation and prevent
browser-derived text from authorising later device, terminal, notification, or
other privileged tool calls.

Only after those host controls and independent SSRF, redirect, DNS-rebinding,
prompt-injection, resource-exhaustion, cancellation, and provenance tests
should its own toolset be added to the `g2` platform. It must remain a separate
package so public-web reading cannot inherit device, active-turn, or proactive
notification authority.

## Authority invariants

1. The WebSocket authenticates a single phone before either MCP session starts.
2. Host-generated session metadata binds platform, profile, chat, event, and
   session context. It never appears in a tool's model-authored input schema.
3. Every active-turn device mutation is checked before discovery, immediately
   before sending, and by the phone again.
4. Proactive calls use a separate explicit allowlist and cannot inherit an old
   foreground turn.
5. Durable mutation operation IDs are generated inside trusted workflow code.
   Unknown outcomes may be retried only with the identical ID and canonical
   payload. Legacy phone mutators without operation-ID support are never
   retried; response loss is reported as an unknown outcome.
6. Thinking, progress, drafts, tool names, and intermediate results are never
   sent to the lens as assistant results.
7. Tool discovery is not authority. Direct calls to hidden/denied tools still
   fail closed.

## SOUL cutover rule

The G2 profile `SOUL.md` may contain persona and optional tone only. It must not
contain tool names, command recipes, device/entity mappings, operation-ID or
retry rules, reminder prompts, browser/provider routes, security policy, output
limits, or transport behavior. Before a section is removed, its behavior must
either be enforced by one of the MCP boundaries above or be deliberately
retired from the public base profile.

## Production deployment gates

The clean-history bridge source is licensed under Apache-2.0 and may be
redistributed. Before representing a packaged build as production-ready,
verify all of the following:

- deployment credentials, private CA/IP/path data, logs, caches, `.venv`, and
  repository metadata are excluded from the build context;
- G2 receives only the minimum MCP/toolsets required for the profile;
- Terminal/debug/sample apps and unnecessary Android permissions are absent
  from the release flavor;
- deterministic reminder outbox and fixed phone delivery pass offline,
  response-loss, restart, duplicate-tick, corrupt-store, and write-failure tests;
- pending reminder text and schedule metadata remain plaintext in the
  profile-local `0600` outbox
  because Hermes exposes no gateway-safe OS-keyring primitive; public release
  must explicitly accept that same-UID privacy limitation or add a supported
  keystore-backed envelope before distribution;
- both MCP roles pass wrong-token, stale-turn, replay, cancellation, reconnect,
  and real phone/glasses tests;
- dependencies are locked, an SBOM is generated, and artifacts are signed.

Until those gates pass, treat packaged builds as development software and do
not represent them as production-ready. These operational gates do not
restrict the rights granted by Apache-2.0.
