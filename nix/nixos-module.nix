self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services."llm-harness";
  tagContainerMapValue =
    lib.concatStringsSep ","
      (lib.mapAttrsToList (tag: container: "${tag}=${container}") cfg.tagContainerMap);
  pluginPythonPath = pkgs.python312Packages.makePythonPath cfg.plugins;
in
{
  options.services."llm-harness" = {
    enable = lib.mkEnableOption "LLM Harness API service";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.llm-harness;
      defaultText = lib.literalExpression "inputs.llm-harness.packages.\${pkgs.system}.default";
      description = "LLM Harness package to run.";
    };

    host = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1";
      description = "Host address for uvicorn to bind.";
    };

    port = lib.mkOption {
      type = lib.types.port;
      default = 8000;
      description = "TCP port for uvicorn to bind.";
    };

    databasePath = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/llm-harness/harness.db";
      description = "SQLite database path used by the harness.";
    };

    eventDatabasePath = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/llm-harness/events.db";
      description = "SQLite database path used by the core eventing engine.";
    };

    openaiBaseUrl = lib.mkOption {
      type = lib.types.str;
      default = "https://api.openai.com/v1";
      description = "OpenAI-compatible base URL for the openai-codex provider.";
    };

    openrouterBaseUrl = lib.mkOption {
      type = lib.types.str;
      default = "https://openrouter.ai/api/v1";
      description = "OpenRouter OpenAI-compatible base URL.";
    };

    chatgptOAuth = {
      authorizationUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "OAuth authorization endpoint for the ChatGPT login plugin.";
      };

      tokenUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "OAuth token endpoint for the ChatGPT login plugin.";
      };

      userinfoUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Optional OIDC UserInfo endpoint for the ChatGPT login plugin.";
      };

      clientId = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "OAuth client id. Put this here only if it is not secret in your setup.";
      };

      redirectUri = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Externally registered callback URI. Defaults to the request URL for /auth/chatgpt/callback.";
      };

      scope = lib.mkOption {
        type = lib.types.str;
        default = "openid profile email";
        description = "OAuth scope requested by /auth/chatgpt/login.";
      };
    };

    codexDeviceOAuth = {
      issuerUrl = lib.mkOption {
        type = lib.types.str;
        default = "https://auth.openai.com";
        description = "OpenAI auth issuer used by the Codex device-code flow.";
      };

      clientId = lib.mkOption {
        type = lib.types.str;
        default = "app_EMoamEEZ73f0CkXaXp7hrann";
        description = "OAuth client id used by the Codex device-code flow.";
      };

      tokenUrl = lib.mkOption {
        type = lib.types.str;
        default = "https://auth.openai.com/oauth/token";
        description = "OAuth token endpoint used by the Codex device-code flow.";
      };

      baseUrl = lib.mkOption {
        type = lib.types.str;
        default = "https://chatgpt.com/backend-api/codex";
        description = "Codex backend base URL associated with the stored OAuth credentials.";
      };

      refreshSkewSeconds = lib.mkOption {
        type = lib.types.ints.positive;
        default = 120;
        description = "Seconds before access-token expiry at which callers should refresh.";
      };
    };

    podmanImage = lib.mkOption {
      type = lib.types.str;
      default = "llm-harness-tool:latest";
      description = "Container image used by the built-in podman-shell tool.";
    };

    podmanImagePackage = lib.mkOption {
      type = lib.types.nullOr lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.podman-tool-image;
      defaultText = lib.literalExpression "inputs.llm-harness.packages.\${pkgs.system}.podman-tool-image";
      description = ''
        Optional Nix-built Podman image archive to load before the service starts.
        Set to null when using an image from a registry or one managed elsewhere.
      '';
    };

    podmanMountNixStore = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Bind-mount /nix/store read-only into tool containers.";
    };

    tagContainerMap = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = {
        project-a = "llm-harness-project-a";
      };
      description = "Map session tags to shared Podman container names.";
    };

    plugins = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [ ];
      example = lib.literalExpression "[ pkgs.my-llm-harness-plugin ]";
      description = ''
        Python packages exposing entry points in the llm_harness.plugins group.
        They are added to PYTHONPATH for the service so provider, tool, and hook
        plugins can be discovered at startup.
      '';
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = {
        HARNESS_OPENAI_BASE_URL = "https://api.openai.com/v1";
      };
      description = "Additional non-secret environment variables for the service.";
    };

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      example = "/run/secrets/llm-harness.env";
      description = ''
        Optional EnvironmentFile for secrets such as HARNESS_OPENAI_API_KEY and
        HARNESS_OPENROUTER_API_KEY. Prefer this over putting API keys in Nix.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "llm-harness";
      description = "User running the service.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "llm-harness";
      description = "Group running the service.";
    };

    enablePodman = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Install Podman and expose it in the service PATH for the podman-shell tool.";
    };

    extraPath = lib.mkOption {
      type = lib.types.listOf lib.types.package;
      default = [ ];
      description = "Extra packages exposed in the service PATH.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.groups.${cfg.group} = { };
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = "/var/lib/llm-harness";
      createHome = true;
    };

    virtualisation.podman.enable = lib.mkIf cfg.enablePodman true;

    systemd.services."llm-harness" = {
      description = "LLM Harness API";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      environment =
        {
          HARNESS_HOST = cfg.host;
          HARNESS_PORT = toString cfg.port;
          HARNESS_DB = toString cfg.databasePath;
          HARNESS_EVENTS_DB = toString cfg.eventDatabasePath;
          HARNESS_OPENAI_BASE_URL = cfg.openaiBaseUrl;
          HARNESS_OPENROUTER_BASE_URL = cfg.openrouterBaseUrl;
          HARNESS_PODMAN_IMAGE = cfg.podmanImage;
          HARNESS_PODMAN_MOUNT_NIX_STORE = if cfg.podmanMountNixStore then "1" else "0";
          HARNESS_TAG_CONTAINER_MAP = tagContainerMapValue;
          PYTHONPATH = pluginPythonPath;
        }
        // lib.optionalAttrs (cfg.chatgptOAuth.authorizationUrl != null) {
          HARNESS_CHATGPT_OAUTH_AUTHORIZATION_URL = cfg.chatgptOAuth.authorizationUrl;
        }
        // lib.optionalAttrs (cfg.chatgptOAuth.tokenUrl != null) {
          HARNESS_CHATGPT_OAUTH_TOKEN_URL = cfg.chatgptOAuth.tokenUrl;
        }
        // lib.optionalAttrs (cfg.chatgptOAuth.userinfoUrl != null) {
          HARNESS_CHATGPT_OAUTH_USERINFO_URL = cfg.chatgptOAuth.userinfoUrl;
        }
        // lib.optionalAttrs (cfg.chatgptOAuth.clientId != null) {
          HARNESS_CHATGPT_OAUTH_CLIENT_ID = cfg.chatgptOAuth.clientId;
        }
        // lib.optionalAttrs (cfg.chatgptOAuth.redirectUri != null) {
          HARNESS_CHATGPT_OAUTH_REDIRECT_URI = cfg.chatgptOAuth.redirectUri;
        }
        // {
          HARNESS_CODEX_OAUTH_ISSUER_URL = cfg.codexDeviceOAuth.issuerUrl;
          HARNESS_CODEX_OAUTH_CLIENT_ID = cfg.codexDeviceOAuth.clientId;
          HARNESS_CODEX_OAUTH_TOKEN_URL = cfg.codexDeviceOAuth.tokenUrl;
          HARNESS_CODEX_OAUTH_BASE_URL = cfg.codexDeviceOAuth.baseUrl;
          HARNESS_CODEX_OAUTH_REFRESH_SKEW_SECONDS = toString cfg.codexDeviceOAuth.refreshSkewSeconds;
          HARNESS_CHATGPT_OAUTH_SCOPE = cfg.chatgptOAuth.scope;
        }
        // cfg.environment;

      path =
        lib.optionals cfg.enablePodman [ pkgs.podman ]
        ++ cfg.extraPath;

      preStart = lib.optionalString (cfg.enablePodman && cfg.podmanImagePackage != null) ''
        podman load -i ${cfg.podmanImagePackage}
      '';

      serviceConfig =
        {
          User = cfg.user;
          Group = cfg.group;
          WorkingDirectory = "/var/lib/llm-harness";
          StateDirectory = "llm-harness";
          ExecStart = "${cfg.package}/bin/llm-harness";
          Restart = "on-failure";
          RestartSec = "5s";
          NoNewPrivileges = true;
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          ReadWritePaths = [ "/var/lib/llm-harness" ];
        }
        // lib.optionalAttrs (cfg.environmentFile != null) {
          EnvironmentFile = cfg.environmentFile;
        };
    };
  };
}
