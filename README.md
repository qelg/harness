# LLM Harness

Ein kleines, erweiterbares Harness fuer session-isolierte LLM-Chats.

## Enthalten

- LLM-Provider als Plugins: eingebaut sind `openai-codex`, `openrouter` und `mock-llm`.
- Tools als Plugins: eingebaut sind `terminal` und `skill_view`.
- Sessions mit Tags als persistente Events in SQLite.
- Messages und Workflow-Zustand als persistente Events in SQLite.
- Streaming-Antworten via Server-Sent Events.
- Event-Consumer-Plugins, die auf EventFilter reagieren koennen.
- Tool-Ausfuehrung session-isoliert in Podman-Containern oder tag-basiert in geteilten Containern.

## Start

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
export HARNESS_OPENAI_API_KEY=...
export HARNESS_OPENROUTER_API_KEY=...
uvicorn llm_harness.api:create_app --factory --reload
```

Die Eventing Engine nutzt standardmaessig eine SQLite-Datei unter `.harness/events.db`.
Das kleine Web-Frontend ist unter `http://127.0.0.1:8000/frontend/` verfuegbar.

## Beispiel

```bash
curl -X POST http://127.0.0.1:8000/sessions \
  -H 'content-type: application/json' \
  -d '{"title":"demo","tags":["project-a"]}'
```

```bash
curl -X POST http://127.0.0.1:8000/sessions/1/messages \
  -H 'content-type: application/json' \
  -d '{"content":"Sag kurz hallo"}'
```

Die persistenten Low-Level-Events einer Session koennen inklusive Tags, Producer und Kausalitaetsmetadaten aufgelistet werden:

```bash
curl http://127.0.0.1:8000/sessions/1/events
```

Optional koennen neue Events fuer eine Session gestreamt werden:

```bash
curl -N http://127.0.0.1:8000/sessions/1/events/stream
```

Modell und Thinking-Level koennen pro Session gewaehlt werden. Erlaubte Thinking-Level sind
`none`, `low`, `medium` und `high`:

```bash
curl -X POST http://127.0.0.1:8000/model-selection \
  -H 'content-type: application/json' \
  -d '{"provider":"openrouter","model":"openai/gpt-5","thinking_level":"medium","session_id":"sess_1"}'
```

```bash
curl -X POST http://127.0.0.1:8000/sessions/1/tools/terminal \
  -H 'content-type: application/json' \
  -d '{"input":{"cmd":"pwd"}}'
```

## Plugin-Schnittstelle

Externe Python-Pakete registrieren Plugins ueber Entry Points:

```toml
[project.entry-points."llm_harness.plugins"]
my_plugin = "my_package.plugin:register"
```

Die `register(registry)`-Funktion kann Provider, Tools, API-Plugins und Event-Consumer registrieren:

```python
def register(registry):
    registry.add_provider(MyProvider())
    registry.add_tool(MyTool())
    registry.add_event_consumer_plugin(MyConsumer())
```

Siehe `llm_harness/protocols.py` fuer die minimalen Interfaces.

Event-Consumer verarbeiten standardmaessig jeweils ein Event gleichzeitig. Die
Parallelitaet kann pro Plugin-Name konfiguriert werden; der persistente Cursor
wird dabei nur bis zum hoechsten lueckenlos fertig verarbeiteten Event
weitergeschoben:

```nix
services."llm-harness".parallelity = {
  terminal = 4;
  "llm-provider-runner" = 2;
};
```

Direkt per Umgebung entspricht dies
`HARNESS_PARALLELITY='{"terminal":4,"llm-provider-runner":2}'`.

## Container-Isolation

`terminal` startet Container nach Bedarf:

- Ohne Mapping: ein Container pro Session.
- Mit `HARNESS_TAG_CONTAINER_MAP=tag-a=name-a,tag-b=name-b`: Sessions mit diesem Tag nutzen den angegebenen Container.
- Image: `HARNESS_PODMAN_IMAGE`, Standard `docker.io/library/python:3.12-slim`.
- Mit `HARNESS_PODMAN_MOUNT_NIX_STORE=1` wird `/nix/store` read-only in neue Tool-Container gemountet.

Podman muss auf dem Host installiert sein.

## Skills

Das eingebaute Tool `skill_view` macht kuratierte Anweisungen und deren Begleitdateien fuer das Modell lesbar. Ein Aufruf mit nur `name` liest `SKILL.md`; mit `file` kann eine Datei relativ zum Skill-Verzeichnis gelesen werden. Optional begrenzen `line_start` und `line_end` die Ausgabe (1-basiert, inklusive):

```json
{"name":"nixos"}
```

```json
{"name":"nixos","file":"references/modules.md","line_start":20,"line_end":80}
```

Die Dateiaufloesung bleibt immer innerhalb des jeweiligen Skill-Verzeichnisses. Konfiguriert werden ein oder mehrere Sammelverzeichnisse; jedes direkte Unterverzeichnis mit einer `SKILL.md` wird automatisch als Skill erkannt. Der Verzeichnisname wird als Skill-Name verwendet und alle erkannten Namen werden in die Tool-Beschreibung aufgenommen:

```text
/srv/skills/
├── nixos/SKILL.md
└── python/SKILL.md
```

Direkt ueber die Umgebung werden die Sammelverzeichnisse als JSON-Liste gesetzt:

```bash
HARNESS_SKILLS='["/srv/skills"]'
```

Die Flake baut ausserdem ein kleines Tool-Image fuer Podman:

```bash
nix build .#podman-tool-image
podman load -i result
```

Auf Hosts ohne globale Containers-Policy kann `podman load` eine Policy-Datei verlangen. `nix run` laedt das Archiv deshalb ueber `skopeo --policy ... copy`; die Policy akzeptiert nur lokale Archive/Verzeichnisse und lehnt andere Transports ab. Fuer manuelles Laden:

```bash
nix build .#podman-tool-image
skopeo --policy "$(nix build --no-link --print-out-paths .#containers-policy)" copy \
  docker-archive:result \
  containers-storage:localhost/llm-harness-tool:latest
```

Das Image heisst `llm-harness-tool:latest`. Es enthaelt im Wesentlichen eine Link-Struktur unter `/bin`, deren Symlinks auf Nix-Store-Pfade zeigen. Deshalb muss der Host-Store in Tool-Container gemountet werden:

```bash
podman run --rm -v /nix/store:/nix/store:ro llm-harness-tool:latest bash -lc 'echo hello'
```

Im NixOS-Modul ist das der Default: `podmanImagePackage` wird vor Service-Start mit `podman load` geladen, `podmanImage = "llm-harness-tool:latest"` gesetzt und `podmanMountNixStore = true` aktiviert.

## Mock LLM

`mock-llm` ist ein eingebauter Provider fuer Tests und lokale Entwicklung ohne LLM-Kosten:

```bash
curl -X POST http://127.0.0.1:8000/sessions/1/messages \
  -H 'content-type: application/json' \
  -d '{"content":"Hallo"}'
```

Die Antwort ist ueber `HARNESS_MOCK_LLM_RESPONSE` konfigurierbar, Default ist `mock llm response`.

## NixOS-Flake

Die Harness kann als Flake-Input eingebunden werden:

```nix
{
  inputs.llm-harness.url = "github:dein-user/llm-harness";

  outputs = { self, nixpkgs, llm-harness, ... }: {
    nixosConfigurations.host = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        llm-harness.nixosModules.default
        {
          services."llm-harness" = {
            enable = true;
            host = "127.0.0.1";
            port = 8000;
            eventDatabasePath = "/var/lib/llm-harness/events.db";
            environmentFile = "/run/secrets/llm-harness.env";

            tagContainerMap = {
              "project-a" = "llm-harness-project-a";
            };

            # Entdeckt z. B. ./skills/nixos/SKILL.md und
            # ./skills/python/SKILL.md automatisch.
            skills = [ ./skills ];

            plugins = [
              # Python-Packages mit Entry Points in "llm_harness.plugins"
              # pkgs.my-llm-harness-plugin
            ];
          };
        }
      ];
    };
  };
}
```

`environmentFile` sollte die Secrets enthalten:

```bash
HARNESS_OPENAI_API_KEY=...
HARNESS_OPENROUTER_API_KEY=...
HARNESS_CHATGPT_OAUTH_CLIENT_SECRET=...
```

Das Modul stellt ausserdem `packages.${system}.default`, `packages.${system}.llm-harness`,
`devShells.${system}.default` und `overlays.default` bereit.

## ChatGPT OAuth Plugin

Das eingebaute API-Plugin `chatgpt-oauth` stellt einen konfigurierbaren OAuth2/OIDC-Flow bereit:

- `GET /auth/chatgpt/login`
- `GET /auth/chatgpt/callback`
- `GET /auth/chatgpt/tokens`
- `POST /auth/chatgpt/tokens/{token_id}/refresh`

Konfiguration erfolgt ueber Env Vars oder das NixOS-Modul:

```bash
HARNESS_CHATGPT_OAUTH_AUTHORIZATION_URL=...
HARNESS_CHATGPT_OAUTH_TOKEN_URL=...
HARNESS_CHATGPT_OAUTH_USERINFO_URL=...
HARNESS_CHATGPT_OAUTH_CLIENT_ID=...
HARNESS_CHATGPT_OAUTH_CLIENT_SECRET=...
HARNESS_CHATGPT_OAUTH_REDIRECT_URI=https://example.test/auth/chatgpt/callback
HARNESS_CHATGPT_OAUTH_SCOPE="openid profile email"
```

Access Token und Refresh Token werden in SQLite gespeichert. Die Listen-API gibt nur Metadaten zurueck, nicht die Tokens selbst.

## OpenAI Codex Device-Code OAuth

Das eingebaute API-Plugin `openai-codex` bildet den Hermes-artigen Device-Code-Flow ab:

- `POST /auth/openai-codex/device/start`
- `POST /auth/openai-codex/device/{device_code_id}/poll`
- `GET /auth/openai-codex/tokens`
- `POST /auth/openai-codex/tokens/{token_id}/refresh`

Start liefert `verification_url` und `user_code`. Der Client zeigt beides an, der Nutzer meldet sich im Browser an, und der Client pollt danach den Poll-Endpunkt. Bei Erfolg werden `access_token` und `refresh_token` in SQLite gespeichert.

Defaults entsprechen dem von Hermes verwendeten Codex-Flow:

```bash
HARNESS_CODEX_OAUTH_ISSUER_URL=https://auth.openai.com
HARNESS_CODEX_OAUTH_CLIENT_ID=app_EMoamEEZ73f0CkXaXp7hrann
HARNESS_CODEX_OAUTH_TOKEN_URL=https://auth.openai.com/oauth/token
HARNESS_CODEX_OAUTH_BASE_URL=https://chatgpt.com/backend-api/codex
HARNESS_CODEX_OAUTH_REFRESH_SKEW_SECONDS=120
```

## Session-Zustaende

Das eingebaute `session-state` Event-Consumer-Plugin projiziert Chat-Aktivitaet in persistente `session.state` Events:

- Eine neue `chat.message.user.created` Nachricht erzeugt `state=running`.
- Eine finale Assistant-Antwort ohne Tool-Call oder ein fehlgeschlagener LLM-Run erzeugt `state=finished` und zunaechst `read=unread`.
- Assistant-Nachrichten mit Tool-Calls und Provider-Ergebnisse, die automatisch wiederholt werden, beenden die Session nicht.

Jedes State-Event traegt `session` und `chat` Tags. `source_event_id` im Payload sowie `causation_id` referenzieren die Nachricht bzw. den fehlgeschlagenen Run, der den Zustand ausgeloest hat. Bei einem fertigen Provider-Ergebnis beschreibt `outcome` den Abschlussgrund, zum Beispiel `stop`, `completed` oder `failed`.

Die fuer eine Uebersicht optimierte API gibt pro Session nur den neuesten Zustand zurueck, absteigend nach letzter Aktivitaet:

```bash
curl http://127.0.0.1:8000/session-states
```

Die vollstaendige State-Historie einer Session ist ebenfalls verfuegbar. Ein fertiger Zustand kann idempotent als gelesen markiert werden. Eine Session wird durch ein neues `session.state` Event mit `archive=true` archiviert. Das naechste regulaere State-Event, beispielsweise nach einer neuen Nachricht, enthaelt dieses Tag nicht mehr und hebt die Archivierung damit automatisch auf:

```bash
curl http://127.0.0.1:8000/sessions/sess_123/state-events
curl -X POST http://127.0.0.1:8000/sessions/sess_123/state/read
curl -X POST http://127.0.0.1:8000/sessions/sess_123/state/archive
```

## Automatische Session-Namen

Das eingebaute `namer` Event-Consumer-Plugin reagiert auf echte Wechsel nach
`running` oder `finished` (nicht auf `archived` und nicht auf reine
`read`-Aenderungen). Fuer jeden Wechsel erzeugt es eine eigene Session mit dem
Tags `namer` und `no-auto-llm-run` sowie dem Event-Tag
`parent_session=<urspruengliche-session>`. Der generische
`no-auto-llm-run`-Tag verhindert, dass User-Nachrichten automatisch den
normalen LLM-Requester starten. Sessions mit einem `parent_session`-Tag gelten
als intern abgeleitet: Sie werden nicht erneut benannt und erscheinen nicht in
der Top-Level-Liste von `GET /sessions`. Direkte abgeleitete Sessions koennen
ueber `GET /sessions/{session_id}/children` geladen werden; die Projektionen
enthalten ihre `parent_session_id`. In die neue Session werden eine
System-Nachricht und eine User-Nachricht geschrieben. Die User-Nachricht enthaelt den bisherigen sichtbaren Verlauf als
`User: ...`/`Assistant: ...`-Transkript und endet mit der Aufforderung, daraus
einen kurzen Titel zu bilden. Tool-Aufrufe und Tool-Antworten werden nicht in
das Transkript aufgenommen. Der Namer-Run bekommt keine Toolsets und fordert
eine ausschliesslich 5-10 Woerter lange Zusammenfassung der gesamten
Konversation als Session-Namen an. Seine Antwort erzeugt ein `session.renamed`
Event in der urspruenglichen Session. `GET /sessions` projiziert jeweils den neuesten
Namen.

Provider und Modell lassen sich direkt per Umgebung konfigurieren:

```bash
HARNESS_NAMER_PROVIDER=openrouter
HARNESS_NAMER_MODEL=openai/gpt-4.1-mini
```

Im NixOS-Modul entsprechen dem `services.llm-harness.namerProvider` und
`services.llm-harness.namerModel`.

## UnifiedPush notifications

The built-in `unifiedpush` event-consumer sends a notification when a top-level
session changes to `finished`. Derived sessions (sessions with a
`parent_session`) and subsequent read/archive state events do not trigger a
notification. The JSON sent to the distributor endpoint contains the session
id, current session title, and final assistant message. Delivery is recorded per
event and subscription so retrying the persistent consumer does not duplicate a
successful push; endpoints returning 404 or 410 are removed.

Android clients register the capability URL supplied by their UnifiedPush
distributor with:

```http
PUT /push/unifiedpush/subscriptions
content-type: application/json

{"instance_id":"stable-device-id","endpoint":"https://push.example/secret"}
```

They unregister it with
`DELETE /push/unifiedpush/subscriptions/{instance_id}`. Only public HTTPS
endpoints are accepted. These routes should be protected by the same access
control as the rest of the Harness API because a UnifiedPush endpoint is a
secret capability URL.

No Google Play service, FCM project, API key, or server-side UnifiedPush service
is required. The Harness host only needs outbound HTTPS access to the endpoint
provided by the phone's distributor.
