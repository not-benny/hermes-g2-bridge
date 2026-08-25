# Hermes G2 Bridge

This repository is the native, authenticated transport between Hermes Agent,
the Hermes G2 Android app, and Even Realities G2 glasses. It registers no
model-facing tools, skills, commands, or workflow prompt policy.

The Python implementation and test suite in this clean-history repository are
newly authored for Hermes G2. Earlier upstream JavaScript source and repository
history are intentionally not included. This project is licensed under the
[Apache License 2.0](LICENSE).

User-intent workflows live in the separate Apache-2.0
`hermes-g2-workflows` Agent Plugin MCP package. The profile `SOUL.md` is
persona-only.

## MCP-only architecture

The phone opens one certificate-validated WSS connection. Its only release
channels are:

- `ctl`: authentication, protocol/capability negotiation, ping/pong, errors;
- `mcp`: private phone-hosted **Device MCP** (Hermes is the client);
- `host-mcp`: Hermes-hosted **Host Session MCP** (the phone is the client).

`host-mcp-v1` is mandatory. A client or gateway without it is closed. Legacy
custom chat, Cockpit, and Companion frames are inert for every client.

Host Session MCP owns `hermes.voice.turn`, exact standard cancellation, and
the bounded `hermes://session/status` resource. The phone receives only the
terminal voice result; thinking, streaming deltas, tool progress, and drafts
never cross as wearer-visible assistant output. Companion is explicitly
unavailable.

Conversate may additionally negotiate `conversate-cues-v1`. Only when the
wearer enables the phone setting, the Host Session MCP lists
`hermes.conversate.cues`. It accepts at most 4,096 Unicode scalars of recent
transcript text, including coalesced live revisions but never audio, and returns at most three exact
`question`, `topic`, or `action` cues. This lane calls the separately
configurable `hermes_g2_conversate_cues` auxiliary model directly with no
agent turn, session history, workflow, device tools, progress UI, or
wearer-visible “Working” state. Requests are latest-wins, support standard MCP
cancellation, and fail closed after 2.5 seconds; the phone silently keeps its
local heuristic cues on every failure. The configured model provider may
process the sent text under its own data policy. Raw auxiliary output is capped
before JSON parsing, cancellation cannot wait indefinitely on a provider that
suppresses task cancellation, and exhausting connection-scoped request-ID
tombstones closes the socket so the normal reconnect path starts a fresh bound.

The Device MCP serves the phone's local registry, but that registry is never
mounted dynamically into the model. The native adapter is its only client. It
pins the reviewed MCP protocol/server identity and exact schema fingerprints
for every route used by a public workflow. Missing or changed tools fail
closed; `tools/listChanged` cannot widen model authority.

## Public workflow MCP

`hermes-g2-workflows` exposes exactly these intent-level tools:

| MCP tool | Intent |
|---|---|
| `g2_work_task_add` | Add one phone-owned Work Tasks item |
| `g2_kanban_task_create` | Create one blocked, unassigned card on one exact existing Hermes Kanban board |
| `g2_clock_set_timer` | Set one durable Clock timer |
| `g2_clock_set_alarm` | Set one durable Clock alarm |
| `g2_reminder_create` | Create one deterministic one-shot reminder |
| `g2_weather_present` | Read typed UKMO weather and present one final deck |
| `g2_train_departures_present` | Read typed National Rail departures and present one final deck |
| `g2_apps_manage` | Launch/list/focus/close apps and manage launcher folders |
| `g2_media_control` | Read now-playing or perform reviewed playback controls |
| `g2_navigation` | Start/stop/read navigation state |
| `g2_notifications` | List or dismiss bounded phone notifications |
| `g2_health_summary` | Read the consent-gated coarse ring-health summary |
| `g2_calendar_agenda` | Read a bounded phone calendar agenda |

There is no generic phone-tool proxy, arbitrary render/card tool, raw state
dump, dynamic app primitive, public completed-result producer, terminal, or
browser/Python execution tool. There is likewise no raw Kanban list, update,
delete, comment, dispatch, or generic proxy surface.

Hermes mints a short-lived HMAC capability for each call. It is bound to the
approved package digest, stable server binding, profile, active G2 turn,
tool-call identity, workflow name, canonical arguments, expiry, and one-use
nonce. The native relay verifies all claims and its replay ledger before a
fixed name-to-handler dispatch. The profile-local Unix socket and same-UID
check are transport isolation, not authority.

Kanban creation matches one active board by exact case-insensitive slug or
display name. A missing or duplicate match returns a typed result containing at
most 16 canonical board choices and performs no mutation; it never falls back
to the phone-owned Work Tasks app or interprets a status as a board. The bridge
revalidates the exact active turn after board enumeration and immediately
before any display-name choices can leave the host. Created cards are always
`blocked` and unassigned. `triage` is deliberately not used,
because Hermes enables its triage auto-decomposer by default. The same create
transaction records the canonical sticky-block event; the initial status alone
would otherwise be promoted by dependency recomputation.

Bridge manifest 2.1.1 adds a current-turn destination fence to this fixed
route. The wearer must name the exact Kanban board in the current turn;
explicit onboard, local, phone, or Work Tasks wording is never Kanban
authority. A missing or ambiguous selection cannot be replaced with a
different Kanban board or the phone's Work Tasks store in the same turn.
Multiple items may still be created for the same authorized destination.
The fence compares the normalized submitted board identity; changing between a
display-name and slug spelling requires a fresh turn even if both later resolve
to the same canonical board.

Before any board write, a
profile-scoped owner-only SQLite ledger records a digest-only `PREPARED` intent,
then durably advances it to `MUTATING`. The bridge holds a bounded POSIX
`flock`, pins the original board slug and filesystem generation, opens that DB
in existing-file-only mode, and revalidates the exact live G2 turn after it has
the canonical board write transaction. The canonical idempotency lookup,
`hermes_cli.kanban_db.create_task` call, sticky-block event, and final
cancellation/authority check share that immediate transaction.

After the board commit, the ledger becomes a permanent `COMMITTED` tombstone.
It survives card assignment, archive, hard deletion, display-name reuse, and
board deletion/recreation. Historical replies report immutable
`created_status: blocked` and `created_assignee: null`; they never claim the
card is still blocked or unassigned. A crash-state `MUTATING` entry may recover
one exact canonical idempotency row, but it never creates when that row is
absent. That deliberate availability tradeoff prevents resurrection after an
unobserved create followed by deletion. Blocking lock/DB work runs outside the
gateway event loop with short lock timeouts, and cancellation/revocation is
checked again inside the still-rollback-capable transaction. The durable lock
contract is POSIX-only and fails closed on hosts without secure `flock`,
`O_NOFOLLOW`, and owner identity checks.
Hermes' special canonical `default` board is advertised before its legacy DB
file exists, so the bridge may run canonical default-board initialization while
authority is current and before recording any operation intent. It never does
this for a missing named board generation. A global-lock timeout is reported as
`operation_outcome_unknown`, because another detached attempt may already be
creating or finalizing the same operation; it is never mislabeled as an
authority failure or `not_committed`.

The reviewed all-in-one installer and exact component locks are published in
[`hermes-g2-distribution`](https://github.com/not-benny/hermes-g2-distribution).

## Deterministic reminders and background delivery

Reminder creation synchronously commits a bounded native outbox. At the due
instant the adapter calls one fixed contracted Device MCP notification route;
it never launches an agent, prompt, cron session, or model. Offline/unknown
outcomes preserve the identical operation ID and inert text for retry, and the
phone's durable queue resolves response loss idempotently.

The outbox lives under an owner-only `state/g2-reminders/` directory with a
`0600` file, symlink rejection, strict JSON decoding, bounded capacity,
same-directory atomic replacement, and fsync. Pending reminder text is
plaintext to the same OS user because Hermes currently has no supported
gateway keystore primitive. A distributable deployment must disclose that
limitation or add a supported keystore-backed envelope.

Background notification authority is internal-only and distinct from active
turn authority. The model cannot invoke or inherit it.

## Typed public data

Train and weather readers use isolated headless Playwright with the real Brave
binary, an ephemeral profile, sandboxing, downloads/popups/dialogs/websockets
blocked, bounded request/time/byte quotas, and fixed provider hosts/routes.
They return typed values rather than raw page bodies. Weather renders the
phone-owned Open-Meteo/UK Met Office attribution.

The UK weather route accepts redundant country qualifiers such as
`Liverpool, UK` without treating `UK` as a county; real county and region
qualifiers still disambiguate same-name places. National Rail requests remain
bound to exact CRS station identities.

Public-data and relay failures emit only fixed stage identifiers. The allowlist
distinguishes capability, active-turn, replay, reader, and revalidation stages
without logging locations, station codes, session claims, exception text, or
caller payloads. This is diagnostic evidence only; authority still fails
closed and user-visible errors remain bounded.

General Browser Harness access is intentionally not part of this authority
package. It requires a separate opt-in public-web MCP with an isolated browser,
read-only typed operations, public-network enforcement, cancellation, quotas,
and no arbitrary Python, raw DOM/page body, personal profile, download, login,
or mutation surface. An undeployed review candidate exists in the sibling
`hermes-public-web` package, but it must remain disabled until the host supplies
container/cgroup-wide resource limits and a taint/user-confirmation boundary
that prevents public-page text from driving privileged follow-on tools. Do not
enable raw `browser_exec` as a substitute.

## Configuration boundary

The native platform needs `HERMES_G2_TOKEN` from the active profile's secret
environment plus a reviewed Tailscale/loopback bind and TLS certificate/key for
non-loopback use. Never place bearer values in committed configuration, logs,
screenshots, or support bundles.

The release profile must:

1. enable this native transport and the portable workflow package;
2. grant trusted session context only to the exact
   `hermes-g2-workflows:workflows` binding and current canonical package
   digest;
3. expose the workflow MCP server explicitly on the G2 platform;
4. keep every raw Device MCP route only in the adapter's private call
   allowlist;
5. exclude native G2 wrapper toolsets, terminal, file/code execution,
   delegation, generic browser execution, and other non-MCP escape routes;
6. keep unrelated MCP servers explicitly allowlisted per platform so the
   workflow package does not default-enable elsewhere.

Run the profile's tool inventory after every install/update. Startup or release
verification must fail if any raw phone/native wrapper is model-visible, the
workflow MCP is absent, the package digest grant is stale, or an unreviewed MCP
server appears.

## Development verification

From this checkout:

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
```

Validate the portable package separately with both system Python and the real
Hermes runtime. Its launch uses the host-controlled absolute Python executable
with `-I -S -B`; capability packages containing `__pycache__` or `.pyc` are
rejected at discovery and again before each capability mint.

Phone verification must cover Host MCP initialize/timeout, status resource,
voice cancellation, optional Conversate cue negotiation/latest-wins/deadline,
disconnect/reconnect, Device MCP cancellation, schema drift, strict
final-frame receipts, and zero legacy custom-channel authority.

## License and release status

This clean-history bridge and the standalone `hermes-g2-workflows` package are
newly authored and licensed under Apache-2.0. The source may be used, modified,
and redistributed under that license.

Before representing a packaged build as production-ready, produce an
allowlisted cache-free artifact, dependency lock and SBOM, provenance record,
compatibility matrix, signed release, secret and private-path scan, and
reproducible install, upgrade, and rollback tests. These operational release
gates do not restrict the rights granted by Apache-2.0.
