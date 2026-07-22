# LLM Harness

Ein kleines, erweiterbares Harness fuer session-isolierte LLM-Chats.

## Enthalten

- LLM-Provider als Plugins: eingebaut sind `openai-codex`, `openrouter` und `mock-llm`.
- Tools als Plugins: eingebaut ist `podman-shell`.
- Sessions mit Tags, persistent in SQLite.
- Messages und Events persistent in SQLite.
- Streaming-Antworten via Server-Sent Events.
- Datenbank-Hooks, die nach Zustandsaenderungen reagieren koennen.
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

Die SQLite-Datei liegt standardmaessig unter `.harness/harness.db`.
Die Eventing Engine nutzt eine eigene SQLite-Datei unter `.harness/events.db`.

## Beispiel

```bash
curl -X POST http://127.0.0.1:8000/sessions \
  -H 'content-type: application/json' \
  -d '{"title":"demo","tags":["project-a"]}'
```

```bash
curl -X POST http://127.0.0.1:8000/sessions/1/messages \
  -H 'content-type: application/json' \
  -d '{"provider":"openrouter","model":"openai/gpt-4.1-mini","content":"Sag kurz hallo"}'
```

Optional koennen Events fuer eine Session gestreamt werden:

```bash
curl -N http://127.0.0.1:8000/sessions/1/events/stream
```

```bash
curl -X POST http://127.0.0.1:8000/sessions/1/tools/podman-shell \
  -H 'content-type: application/json' \
  -d '{"input":{"cmd":"pwd"}}'
```

## Plugin-Schnittstelle

Externe Python-Pakete registrieren Plugins ueber Entry Points:

```toml
[project.entry-points."llm_harness.plugins"]
my_plugin = "my_package.plugin:register"
```

Die `register(registry)`-Funktion kann Provider, Tools und Hooks registrieren:

```python
def register(registry):
    registry.add_provider(MyProvider())
    registry.add_tool(MyTool())
    registry.add_hook(MyHook())
```

Siehe `llm_harness/protocols.py` fuer die minimalen Interfaces.

## Container-Isolation

`podman-shell` startet Container nach Bedarf:

- Ohne Mapping: ein Container pro Session.
- Mit `HARNESS_TAG_CONTAINER_MAP=tag-a=name-a,tag-b=name-b`: Sessions mit diesem Tag nutzen den angegebenen Container.
- Image: `HARNESS_PODMAN_IMAGE`, Standard `docker.io/library/python:3.12-slim`.
- Mit `HARNESS_PODMAN_MOUNT_NIX_STORE=1` wird `/nix/store` read-only in neue Tool-Container gemountet.

Podman muss auf dem Host installiert sein.

Die Flake baut ausserdem ein kleines Tool-Image fuer Podman:

```bash
nix build .#podman-tool-image
podman load -i result
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
  -d '{"provider":"mock-llm","model":"test","content":"Hallo"}'
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
            databasePath = "/var/lib/llm-harness/harness.db";
            environmentFile = "/run/secrets/llm-harness.env";

            tagContainerMap = {
              "project-a" = "llm-harness-project-a";
            };

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
