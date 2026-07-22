# Architektur

Dieses Dokument beschreibt den aktuellen Stand der LLM Harness. Die Anwendung ist ein kleines, session-orientiertes API-System fuer LLM-Chats, Plugin-Provider, Plugin-Tools, Datenbank-Hooks und optionale Podman-Isolation.

## Ziele

- LLM-Anbieter sollen austauschbar sein. Eingebaut sind OpenAI-kompatible Provider fuer `openai-codex` und `openrouter` sowie `mock-llm` fuer Tests.
- Tools sollen als Plugins registriert werden koennen.
- Chats sollen persistent sein und Tags tragen, damit sie Projekten, Workloads oder Container-Zielen zugeordnet werden koennen.
- Zustandsaenderungen sollen Events erzeugen. Plugins koennen ueber Hooks auf diese Events reagieren.
- Streaming-Antworten sollen sofort an Clients geliefert und nach Abschluss als Message gespeichert werden.
- Tool-Ausfuehrung soll pro Session isolierbar sein. Das eingebaute `podman-shell` Tool nutzt pro Session einen eigenen Container, sofern kein Tag-Mapping auf einen geteilten Container konfiguriert ist.

## Komponenten

| Komponente | Datei | Verantwortung |
| --- | --- | --- |
| App Factory | `llm_harness/api.py` | Composition Root: Store, EventBus und Registry erzeugen, Plugins installieren. |
| Core API Plugin | `llm_harness/api_plugin.py` | HTTP-Endpunkte, SSE-Streaming vom Event-Bus und Schreiben von User-/Tool-Requests. |
| Store | `llm_harness/db.py` | SQLite-Verbindung, Schema-Initialisierung, persistente Sessions, Messages und Events. |
| Domain | `llm_harness/domain.py` | Dataclasses und Enums fuer Sessions, Messages, Events und Tool-Aufrufe. |
| Core Event Types | `llm_harness/core/types.py` | Zentrale Core-Eventnamen, Tagtypen, Pflicht-Tags und neue `sess_...` Session-IDs. |
| Plugin Registry | `llm_harness/plugins.py` | Registrierung und Lookup von LLM-Providern, Tools und Datenbank-Hooks. |
| Eventing Engine | `llm_harness/events.py` | Eigene SQLite-Event-DB, monotone Event-IDs, Batch-Append, Tags, Replay und Fan-out. |
| Plugin Protokolle | `llm_harness/protocols.py` | Minimale Python-Interfaces fuer Provider, Tools und Hooks. |
| Builtins | `llm_harness/builtin.py` | Registrierung der eingebauten OpenAI-kompatiblen Provider und des Podman-Tools. |
| OpenAI-kompatibler Provider | `llm_harness/providers/openai_compatible.py` | Streaming gegen `/chat/completions` kompatible APIs. |
| Mock LLM Provider | `llm_harness/providers/mock.py` | Deterministischer Streaming-Provider fuer Tests ohne LLM-Kosten. |
| Podman Tool | `llm_harness/tools/podman_shell.py` | Shell-Ausfuehrung in session- oder tag-gebundenen Podman-Containern. |
| ChatGPT OAuth Plugin | `llm_harness/auth_plugins/chatgpt_oauth.py` | Konfigurierbarer OAuth2/OIDC Login-Flow mit Access-/Refresh-Token-Speicherung. |
| OpenAI Codex Device OAuth Plugin | `llm_harness/auth_plugins/openai_codex_device.py` | Hermes-artiger OpenAI-Codex-Device-Code-Flow. |
| CLI Entrypoint | `llm_harness/__main__.py` | Startet Uvicorn und liest `HARNESS_HOST`/`HARNESS_PORT`. |
| Nix Flake | `flake.nix` | Paket, Dev-Shell, Checks, Overlay und NixOS-Modul-Export. |
| NixOS Modul | `nix/nixos-module.nix` | Systemd-Service und konfigurierbare Runtime-Umgebung inklusive Plugins. |
| Podman Tool Image | `nix/podman-tool-image.nix` | Minimaler Nix-gebauter Container mit `/bin`-Link-Tree auf Host-Nix-Store-Pfade. |

## Laufzeitmodell

Beim Start der API passiert:

1. `Settings.from_env()` liest Runtime-Konfiguration aus Umgebungsvariablen.
2. `Store(settings.database_path)` oeffnet SQLite, aktiviert Foreign Keys und WAL.
3. `store.init_schema()` legt fehlende Tabellen und Indizes an.
4. `Registry()` wird erstellt.
5. `load_plugins(registry)` registriert zuerst Builtins und danach Python Entry Points aus `llm_harness.plugins`.
6. `EventService(settings.event_database_path)` wird erstellt.
7. Die FastAPI-App speichert `store`, `bus` und `registry` in `app.state`.
8. Alle API-Plugins installieren ihre Routen. Auch die Kern-HTTP-API ist ein Plugin: `harness-api`.

Die Registry ist aktuell pro Prozess in-memory. Persistenter Zustand liegt in SQLite. Plugins werden beim Prozessstart entdeckt; dynamisches Hot-Reloading von Plugins gibt es noch nicht.

Das Zielmodell ist: API und Plugins schreiben Events in den EventService. Der EventService weist monotone Millisekunden-IDs zu, persistiert in `events.db`, acknowledged erst nach Commit und dispatched danach an Abonnenten. Bestehende State-Tabellen sind bis zur weiteren Migration noch direkte Tabellen, sollen aber schrittweise zu Projektionen werden.

API-Plugins koennen beim Start eigene Routen und eigene Tabellen installieren. Das eingebaute `chatgpt-oauth` Plugin nutzt diese Erweiterung.

## API-Flows

### Session erstellen

`POST /sessions`

1. API nimmt `title` und `tags` entgegen.
2. Store normalisiert Tags: Whitespace wird entfernt, leere Tags werden verworfen, Duplikate bleiben nur einmal erhalten.
3. Store schreibt `sessions`.
4. Store schreibt ein `session.created` Event.
5. Registry ruft alle Hooks mit diesem Event auf.
6. API gibt die Session zurueck.

### Nachricht schreiben

`POST /sessions/{session_id}/messages`

1. API validiert die Session.
2. API schreibt `chat.message.user.created`.
3. Das `llm-run-requester` Plugin reagiert auf die User-Message und schreibt `llm.run.requested` mit der aktuellen Modell-Auswahl.
4. Das `llm-provider-runner` Plugin reagiert auf `llm.run.requested` fuer den passenden Provider.
5. Der Provider-Runner rekonstruiert den bisherigen Verlauf aus `chat.message.*.created` Events der Session.
6. Der Provider-Runner schreibt `llm.run.started`, publiziert transiente `llm.delta` Events und schreibt nach Abschluss `chat.message.assistant.created`.

### Events streamen

`GET /sessions/{session_id}/events/stream`

1. API abonniert den Event-Bus.
2. Persistierte DB-Events und transiente Streaming-Events werden als SSE ausgegeben.
3. Bei Leerlauf sendet die API `heartbeat`.

### Nachricht schreiben und Events streamen

`POST /sessions/{session_id}/messages/stream`

Dieser Endpoint ist nur eine Komfortkombination aus `POST /messages` und Event-Bus-Streaming. Er ruft keinen Provider direkt auf. Er schreibt zuerst die User-Message, streamt danach Bus-Events fuer die Session und endet bei `chat.message.assistant.created` oder `llm.run.failed`.

Wenn waehrend des Provider-Streams ein Fehler auftritt, schreibt der Provider-Runner ein persistiertes `llm.run.failed` Event. Unvollstaendige Assistant-Deltas werden nicht persistent gespeichert.

### Tool ausfuehren

`POST /sessions/{session_id}/tools/{tool_name}`

1. API validiert die Session.
2. Store schreibt `tool.requested` mit Tool-Name und Input.
3. Registry publiziert das persistierte Event auf dem Event-Bus.
4. Der `tool-request` Hook reagiert auf das Event.
5. Der Hook schreibt `tool.started`, fuehrt das Tool aus und speichert erfolgreiches Tool-Output als `tool` Message.
6. Danach schreibt der Hook `tool.finished`.
7. Bei Fehlern schreibt der Hook `tool.failed`.

Die API gibt nur `accepted` und das `tool.requested` Event zurueck. Ergebnisse kommen ueber `GET /messages` oder den Event-Stream.

## Plugin-Modell

Plugins sind Python-Pakete mit Entry Points in der Gruppe `llm_harness.plugins`:

```toml
[project.entry-points."llm_harness.plugins"]
my_plugin = "my_package.plugin:register"
```

Die referenzierte Funktion bekommt die Registry:

```python
def register(registry):
    registry.add_provider(MyProvider())
    registry.add_tool(MyTool())
    registry.add_hook(MyHook())
```

### LLMProvider

```python
class LLMProvider(Protocol):
    name: str

    async def stream_chat(self, *, model: str, messages: Sequence[Message]) -> AsyncIterator[str]:
        ...
```

Ein Provider liefert Text-Deltas als Async Iterator. Der `llm-provider-runner` Consumer ist fuer transiente Delta-Events und Persistenz des finalen Assistant-Texts verantwortlich.

### Tool

```python
class Tool(Protocol):
    name: str

    async def run(self, call: ToolCall) -> ToolResult:
        ...
```

Ein Tool erhaelt Session-Kontext, Tool-Namen und beliebiges JSON-kompatibles Input. Das Ergebnis wird als `tool` Message gespeichert; Metadaten landen in `metadata_json`.

### DatabaseHook

```python
class DatabaseHook(Protocol):
    name: str

    async def on_event(self, event: Event, *, store: Store, bus: EventBus, registry: Registry) -> None:
        ...
```

Hooks werden nach Store-Operationen aufgerufen. Die Datenbanktransaktion ist zu diesem Zeitpunkt abgeschlossen. Hooks duerfen asynchron sein oder synchron `None` zurueckgeben. Sie bekommen Store, Event-Bus und Registry explizit als Kontext.

Standardmaessig laufen Hooks als Hintergrundtasks. Fuer Tests kann `HARNESS_WORKERS_INLINE=1` gesetzt werden, dann wartet die API auf Hooks.

### ApiPlugin

```python
class ApiPlugin(Protocol):
    name: str

    def install_api(self, *, app: FastAPI, store: Store, bus: EventBus, registry: Registry) -> None:
        ...
```

Ein API-Plugin darf FastAPI-Routen installieren und eigene Datenbankschemata initialisieren. Der aktuelle Store wird uebergeben, damit Plugin-Tabellen dieselbe SQLite-Datei nutzen koennen.

## Eingebaute Provider

Beide eingebauten Provider nutzen denselben OpenAI-kompatiblen Adapter:

| Registry-Name | Base URL Env | API Key Env |
| --- | --- | --- |
| `openai-codex` | `HARNESS_OPENAI_BASE_URL`, Default `https://api.openai.com/v1` | `HARNESS_OPENAI_API_KEY` oder `OPENAI_API_KEY` |
| `openrouter` | `HARNESS_OPENROUTER_BASE_URL`, Default `https://openrouter.ai/api/v1` | `HARNESS_OPENROUTER_API_KEY` oder `OPENROUTER_API_KEY` |
| `mock-llm` | keine externe Base URL | kein API-Key |

Der Adapter ruft `POST {base_url}/chat/completions` mit `stream: true` auf und wertet Server-Sent-Event-Zeilen im Format `data: ...` aus.

`mock-llm` nutzt keine externe API. Er streamt die konfigurierte Antwort aus `HARNESS_MOCK_LLM_RESPONSE`, Default `mock llm response`.

## Eingebautes Tool: podman-shell

`podman-shell` erwartet Input:

```json
{
  "cmd": "pwd",
  "timeout": 30
}
```

Container-Auswahl:

1. Fuer jedes Session-Tag wird `HARNESS_TAG_CONTAINER_MAP` geprueft.
2. Falls ein Tag gemappt ist, wird der konfigurierte Containername genutzt.
3. Andernfalls wird `llm-harness-session-{session_id}` genutzt.

Wenn der Container nicht existiert, startet das Tool:

```bash
podman run -d --name <name> --label llm-harness=true <image> sleep infinity
```

Wenn `HARNESS_PODMAN_MOUNT_NIX_STORE=1` gesetzt ist, fuegt das Tool hinzu:

```bash
--volume /nix/store:/nix/store:ro
```

Danach wird ausgefuehrt:

```bash
podman exec <name> sh -lc <cmd>
```

Das Tool prueft nur Container-Namen und delegiert Shell-Semantik bewusst an `sh -lc` im Container.

### Nix-gebautes Tool-Image

Die Flake exportiert `packages.${system}.podman-tool-image`. Das Image heisst `llm-harness-tool:latest` und enthaelt eine kleine Rootfs:

- `/bin` enthaelt Symlinks auf Programme aus `bashInteractive`, `coreutils`, `findutils`, `gnugrep` und `gnused`.
- `/bin/sh` und `/bin/bash` zeigen auf Bash.
- `/usr/bin/env` zeigt auf `/bin/env`.
- `/tmp` ist vorhanden und world-writable.

Das Image ist fuer Hosts gedacht, die `/nix/store` read-only in den Container mounten. Die Symlinks zeigen auf Store-Pfade und funktionieren daher nur, wenn der Host-Store im Container sichtbar ist.

## Eingebautes API-Plugin: chatgpt-oauth

`chatgpt-oauth` implementiert einen konfigurierbaren OAuth2/OIDC Authorization-Code-Flow:

| Endpoint | Bedeutung |
| --- | --- |
| `GET /auth/chatgpt/login` | Erzeugt `state`, speichert ihn kurzlebig und leitet zum Authorization Endpoint weiter. |
| `GET /auth/chatgpt/callback` | Validiert `state`, tauscht `code` gegen Tokens und speichert Access-/Refresh-Token. |
| `GET /auth/chatgpt/tokens` | Listet Token-Metadaten ohne geheime Tokenwerte. |
| `POST /auth/chatgpt/tokens/{token_id}/refresh` | Nutzt das gespeicherte Refresh Token, um das Access Token zu erneuern. |

Der Flow ist bewusst endpoint-konfigurierbar, weil die oeffentlich auffindbare OpenAI-Plattformdokumentation API-Key-Auth beschreibt und keinen stabil dokumentierten generischen `Login with ChatGPT` OAuth Endpoint fuer Drittanwendungen vorgibt.

## Eingebautes API-Plugin: openai-codex device OAuth

`openai-codex` bildet den von Hermes verwendeten OpenAI-Codex-Device-Code-Flow ab:

| Endpoint | Bedeutung |
| --- | --- |
| `POST /auth/openai-codex/device/start` | Fordert `device_auth_id` und `user_code` bei OpenAI an und speichert den kurzlebigen Login-Vorgang. |
| `POST /auth/openai-codex/device/{device_code_id}/poll` | Pollt OpenAI. `403`/`404` bedeuten weiter warten; `200` liefert Authorization Code und PKCE-Verifier. |
| `GET /auth/openai-codex/tokens` | Listet gespeicherte Codex-Token-Metadaten ohne geheime Tokenwerte. |
| `POST /auth/openai-codex/tokens/{token_id}/refresh` | Erneuert das Access Token mit dem gespeicherten Refresh Token. |

Der Flow nutzt folgende OpenAI-spezifische Endpunkte:

| Zweck | Default |
| --- | --- |
| Issuer | `https://auth.openai.com` |
| User-Code Request | `https://auth.openai.com/api/accounts/deviceauth/usercode` |
| Polling | `https://auth.openai.com/api/accounts/deviceauth/token` |
| Browser Verification | `https://auth.openai.com/codex/device` |
| Token Exchange / Refresh | `https://auth.openai.com/oauth/token` |
| Client ID | `app_EMoamEEZ73f0CkXaXp7hrann` |
| Device Callback URI | `https://auth.openai.com/deviceauth/callback` |

Anders als RFC-8628-Device-Code-Flows liefert das Polling nicht direkt Tokens, sondern einen Authorization Code plus PKCE `code_verifier`. Die Harness tauscht diese Werte anschliessend am Token Endpoint gegen `access_token` und `refresh_token`.

## Datenbankschema

SQLite wird im WAL-Modus betrieben. Zeitstempel werden als ISO-8601 Text gespeichert.

### `sessions`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel der Chat-Session. |
| `title` | `TEXT` | Ja | `NULL` | Optionaler Anzeigename der Session. |
| `tags_json` | `TEXT` | Nein | keiner | JSON-Array normalisierter Tags. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |
| `updated_at` | `TEXT` | Nein | keiner | Letzte Aktualisierung als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  tags_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

### `messages`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel der Message. |
| `session_id` | `INTEGER` | Nein | keiner | Fremdschluessel auf `sessions.id`. |
| `role` | `TEXT` | Nein | keiner | Eine Rolle aus `system`, `user`, `assistant`, `tool`. |
| `content` | `TEXT` | Nein | keiner | Nachrichteninhalt oder Tool-Output. |
| `provider` | `TEXT` | Ja | `NULL` | Provider-Name fuer LLM-bezogene Messages. |
| `model` | `TEXT` | Ja | `NULL` | Modellname fuer LLM-bezogene Messages. |
| `metadata_json` | `TEXT` | Nein | keiner | JSON-Objekt fuer Tool-Metadaten oder spaetere Erweiterungen. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  provider TEXT,
  model TEXT,
  metadata_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
  ON messages(session_id, created_at);
```

### `events`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel des Events. |
| `session_id` | `INTEGER` | Ja | `NULL` | Optionaler Fremdschluessel auf `sessions.id`. |
| `type` | `TEXT` | Nein | keiner | Event-Typ, z. B. `message.created`. |
| `payload_json` | `TEXT` | Nein | keiner | JSON-Objekt mit Event-spezifischen Daten. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER REFERENCES sessions(id) ON DELETE SET NULL,
  type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_session_created
  ON events(session_id, created_at);
```

### `oauth_states`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel des OAuth-State-Eintrags. |
| `provider` | `TEXT` | Nein | keiner | Plugin-/Provider-Name, aktuell `chatgpt-oauth`. |
| `state` | `TEXT` | Nein | keiner | CSRF-State fuer Authorization-Code-Callback. |
| `return_to` | `TEXT` | Ja | `NULL` | Optionales Ziel, das nach Callback an den Client zurueckgegeben wird. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |
| `expires_at` | `TEXT` | Nein | keiner | Ablaufzeitpunkt als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS oauth_states (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  state TEXT NOT NULL UNIQUE,
  return_to TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oauth_states_provider_state
  ON oauth_states(provider, state);
```

### `oauth_tokens`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel des gespeicherten Tokensatzes. |
| `provider` | `TEXT` | Nein | keiner | Plugin-/Provider-Name, aktuell `chatgpt-oauth`. |
| `subject` | `TEXT` | Nein | keiner | User-Subject aus UserInfo, Token Response oder `id_token`. |
| `access_token` | `TEXT` | Nein | keiner | OAuth Access Token. |
| `refresh_token` | `TEXT` | Ja | `NULL` | OAuth Refresh Token, sofern geliefert. |
| `token_type` | `TEXT` | Ja | `NULL` | Token Type, typischerweise `Bearer`. |
| `scope` | `TEXT` | Ja | `NULL` | Vom Token Endpoint bestaetigter Scope. |
| `expires_at` | `TEXT` | Ja | `NULL` | Berechneter Ablaufzeitpunkt aus `expires_in`. |
| `metadata_json` | `TEXT` | Nein | keiner | JSON-Objekt mit UserInfo und nicht geheimem Token Response Rest. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |
| `updated_at` | `TEXT` | Nein | keiner | Letzte Aktualisierung als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS oauth_tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  subject TEXT NOT NULL,
  access_token TEXT NOT NULL,
  refresh_token TEXT,
  token_type TEXT,
  scope TEXT,
  expires_at TEXT,
  metadata_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oauth_tokens_provider_subject
  ON oauth_tokens(provider, subject);
```

### `oauth_device_codes`

| Spalte | Typ | Null | Default | Bedeutung |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Nein | `AUTOINCREMENT` | Primaerschluessel des Device-Code-Vorgangs. |
| `provider` | `TEXT` | Nein | keiner | Provider-Name, aktuell `openai-codex`. |
| `device_auth_id` | `TEXT` | Nein | keiner | OpenAI Device Auth ID. |
| `user_code` | `TEXT` | Nein | keiner | Code, den der Nutzer im Browser eingibt. |
| `verification_url` | `TEXT` | Nein | keiner | Browser-URL fuer die Anmeldung. |
| `interval_seconds` | `INTEGER` | Nein | keiner | Empfohlenes Polling-Intervall. |
| `created_at` | `TEXT` | Nein | keiner | Erstellzeitpunkt als ISO-8601 UTC-String. |
| `expires_at` | `TEXT` | Nein | keiner | Ablaufzeitpunkt als ISO-8601 UTC-String. |

DDL:

```sql
CREATE TABLE IF NOT EXISTS oauth_device_codes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  device_auth_id TEXT NOT NULL UNIQUE,
  user_code TEXT NOT NULL,
  verification_url TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oauth_device_codes_provider
  ON oauth_device_codes(provider, expires_at);
```

## Event-Typen

| Typ | Ausloeser | Payload |
| --- | --- | --- |
| `session.created` | Neue Session wurde gespeichert. | `{"tags": [...]}` |
| `message.created` | User-, Assistant- oder Tool-Message wurde gespeichert. | `{"message_id": ..., "role": ..., "provider": ..., "model": ...}` |
| `tool.requested` | API oder Plugin fordert Tool-Ausfuehrung an. | `{"tool": ..., "input": {...}}` |
| `tool.started` | Tool-Aufruf beginnt. | `{"tool": ...}` |
| `tool.finished` | Tool-Aufruf war erfolgreich. | `{"tool": ..., "message_id": ..., "metadata": {...}}` |
| `tool.failed` | Tool-Aufruf ist fehlgeschlagen. | `{"tool": ..., "error": ...}` |

Provider-Streaming-Events:

| Typ | Ausloeser | Payload |
| --- | --- | --- |
| `llm.run.started` | `llm-provider-runner` beginnt Provider-Aufruf. | `{"provider": ..., "model": ..., "run_id": ...}` |
| `llm.delta` | Provider liefert Textfragment. Transient, nicht in SQLite gespeichert. | `{"provider": ..., "model": ..., "run_id": ..., "delta": ..., "sequence": ...}` |
| `chat.message.assistant.created` | Assistant-Message wurde final gespeichert. | `{"content": ..., "provider": ..., "model": ..., "run_id": ...}` |
| `llm.run.failed` | Provider-Aufruf ist fehlgeschlagen. | `{"error": ..., "provider": ..., "model": ..., "run_id": ...}` |

## Konfiguration

Runtime-Konfiguration kommt aus Umgebungsvariablen:

| Variable | Bedeutung | Default |
| --- | --- | --- |
| `HARNESS_DB` | SQLite-Datei | `.harness/harness.db` |
| `HARNESS_EVENTS_DB` | SQLite-Datei fuer die Eventing Engine | `.harness/events.db` |
| `HARNESS_HOST` | Uvicorn Bind Host | `127.0.0.1` |
| `HARNESS_PORT` | Uvicorn Bind Port | `8000` |
| `HARNESS_OPENAI_API_KEY` | API-Key fuer `openai-codex` | keiner |
| `OPENAI_API_KEY` | Fallback API-Key fuer `openai-codex` | keiner |
| `HARNESS_OPENAI_BASE_URL` | Base URL fuer `openai-codex` | `https://api.openai.com/v1` |
| `HARNESS_OPENROUTER_API_KEY` | API-Key fuer `openrouter` | keiner |
| `OPENROUTER_API_KEY` | Fallback API-Key fuer `openrouter` | keiner |
| `HARNESS_OPENROUTER_BASE_URL` | Base URL fuer `openrouter` | `https://openrouter.ai/api/v1` |
| `HARNESS_PODMAN_IMAGE` | Image fuer neue Tool-Container | `docker.io/library/python:3.12-slim` |
| `HARNESS_PODMAN_MOUNT_NIX_STORE` | Mountet `/nix/store` read-only in neue Tool-Container | `0` |
| `HARNESS_TAG_CONTAINER_MAP` | Kommagetrennte Tag-Container-Zuordnung | leer |
| `HARNESS_CHATGPT_OAUTH_AUTHORIZATION_URL` | OAuth Authorization Endpoint fuer `chatgpt-oauth` | keiner |
| `HARNESS_CHATGPT_OAUTH_TOKEN_URL` | OAuth Token Endpoint fuer `chatgpt-oauth` | keiner |
| `HARNESS_CHATGPT_OAUTH_USERINFO_URL` | Optionaler OIDC UserInfo Endpoint | keiner |
| `HARNESS_CHATGPT_OAUTH_CLIENT_ID` | OAuth Client ID | keiner |
| `HARNESS_CHATGPT_OAUTH_CLIENT_SECRET` | OAuth Client Secret | keiner |
| `HARNESS_CHATGPT_OAUTH_REDIRECT_URI` | Explizite Callback URL | Request-URL |
| `HARNESS_CHATGPT_OAUTH_SCOPE` | Angeforderter OAuth Scope | `openid profile email` |
| `HARNESS_CODEX_OAUTH_ISSUER_URL` | OpenAI Auth Issuer fuer Device-Code-Flow | `https://auth.openai.com` |
| `HARNESS_CODEX_OAUTH_CLIENT_ID` | Codex OAuth Client ID | `app_EMoamEEZ73f0CkXaXp7hrann` |
| `HARNESS_CODEX_OAUTH_TOKEN_URL` | Codex OAuth Token Endpoint | `https://auth.openai.com/oauth/token` |
| `HARNESS_CODEX_OAUTH_BASE_URL` | Codex Backend Base URL | `https://chatgpt.com/backend-api/codex` |
| `HARNESS_CODEX_OAUTH_REFRESH_SKEW_SECONDS` | Refresh-Sicherheitsfenster | `120` |
| `HARNESS_MOCK_LLM_RESPONSE` | Antwort des `mock-llm` Providers | `mock llm response` |
| `HARNESS_WORKERS_INLINE` | Fuehrt Hooks synchron aus, vor allem fuer Tests | `0` |

`HARNESS_TAG_CONTAINER_MAP` hat das Format:

```text
tag-a=container-a,tag-b=container-b
```

## NixOS-Modul

Die Flake exportiert `nixosModules.default`. Das Modul definiert `services."llm-harness"` und startet die Anwendung als systemd-Service.

Wichtige Optionen:

| Option | Typ | Bedeutung |
| --- | --- | --- |
| `enable` | Bool | Aktiviert den Service. |
| `package` | Package | Zu startendes Harness-Paket. |
| `host` | String | Bind Host. |
| `port` | Port | Bind Port. |
| `databasePath` | String | SQLite-Pfad. |
| `eventDatabasePath` | String | SQLite-Pfad fuer die Eventing Engine. |
| `openaiBaseUrl` | String | Base URL fuer `openai-codex`. |
| `openrouterBaseUrl` | String | Base URL fuer `openrouter`. |
| `chatgptOAuth.authorizationUrl` | String oder `null` | OAuth Authorization Endpoint. |
| `chatgptOAuth.tokenUrl` | String oder `null` | OAuth Token Endpoint. |
| `chatgptOAuth.userinfoUrl` | String oder `null` | Optionaler UserInfo Endpoint. |
| `chatgptOAuth.clientId` | String oder `null` | OAuth Client ID. |
| `chatgptOAuth.redirectUri` | String oder `null` | Explizite Callback URL. |
| `chatgptOAuth.scope` | String | Angeforderter Scope. |
| `codexDeviceOAuth.issuerUrl` | String | OpenAI Auth Issuer. |
| `codexDeviceOAuth.clientId` | String | Codex OAuth Client ID. |
| `codexDeviceOAuth.tokenUrl` | String | Token Endpoint fuer Exchange und Refresh. |
| `codexDeviceOAuth.baseUrl` | String | Codex Backend Base URL fuer gespeicherte Token-Metadaten. |
| `codexDeviceOAuth.refreshSkewSeconds` | Positive Integer | Refresh-Sicherheitsfenster in Sekunden. |
| `podmanImage` | String | Image fuer neue Podman-Tool-Container. |
| `podmanImagePackage` | Package oder `null` | Optionales Nix-gebautes Image-Archiv, das vor Service-Start mit `podman load` geladen wird. |
| `podmanMountNixStore` | Bool | Mountet `/nix/store` read-only in Tool-Container. |
| `tagContainerMap` | Attrset von String | Tag-Container-Mapping. |
| `plugins` | Liste von Packages | Python-Plugin-Pakete fuer `PYTHONPATH`. |
| `environment` | Attrset von String | Zusaetzliche nicht geheime Env Vars. |
| `environmentFile` | Pfad oder `null` | Secret-Datei fuer API-Keys. |
| `enablePodman` | Bool | Aktiviert Podman und nimmt es in den Service-PATH auf. |
| `extraPath` | Liste von Packages | Zusaetzliche Programme im Service-PATH. |

Plugins werden ueber `services."llm-harness".plugins` als Python-Packages eingebunden. Das Modul erzeugt daraus einen `PYTHONPATH` mit `python312Packages.makePythonPath`.

## Bewusste Grenzen des aktuellen MVP

- Keine Datenbankmigrationen; `init_schema()` legt nur fehlende Tabellen/Indizes an.
- Tags sind als JSON-Array in `sessions.tags_json` gespeichert. Das ist einfach, aber fuer komplexe Tag-Abfragen weniger effizient als eine normalisierte Join-Tabelle.
- Streaming-Deltas werden erst nach erfolgreichem Abschluss als Assistant-Message gespeichert. Teilantworten ueberleben aktuell keinen Prozessabbruch.
- Hooks laufen inline und haben keine Retry-/Queue-Semantik.
- SQLite-Verbindung ist pro Prozess und `check_same_thread=False`; fuer hoeheren Parallelismus waere ein expliziter Connection-/Transaction-Ansatz sinnvoll.
- Podman-Container-Lifecycle kennt aktuell Start-on-demand, aber kein automatisches Aufraeumen.
- Plugin-Konfiguration erfolgt derzeit ueber Paketinstallation und Environment, nicht ueber eine eigene Plugin-Konfigurationsdatenbank.
- OAuth Tokens werden aktuell im Klartext in SQLite gespeichert. Fuer produktive Nutzung sollte Verschluesselung oder ein Secret-Store ergaenzt werden.
