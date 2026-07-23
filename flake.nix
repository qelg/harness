{
  description = "Session-oriented LLM harness with event-driven plugin providers, tools, and streaming.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      forAllSystems =
        f:
        nixpkgs.lib.genAttrs supportedSystems (
          system:
          let
            pkgs = import nixpkgs { inherit system; };
            python = pkgs.python312.override {
              packageOverrides = pyFinal: pyPrev: {
                inline-snapshot = pyPrev.inline-snapshot.overridePythonAttrs (_old: {
                  doCheck = false;
                });
              };
            };
            pythonPackages = python.pkgs;
          in
          f {
            inherit system pkgs python pythonPackages;
          }
        );
    in
    {
      packages = forAllSystems (
        { pkgs, pythonPackages, system, ... }:
        let
          localContainersPolicy = pkgs.writeText "llm-harness-containers-policy.json" ''
            {
              "default": [
                {
                  "type": "reject"
                }
              ],
              "transports": {
                "docker-archive": {
                  "": [
                    {
                      "type": "insecureAcceptAnything"
                    }
                  ]
                },
                "oci-archive": {
                  "": [
                    {
                      "type": "insecureAcceptAnything"
                    }
                  ]
                },
                "oci": {
                  "": [
                    {
                      "type": "insecureAcceptAnything"
                    }
                  ]
                },
                "dir": {
                  "": [
                    {
                      "type": "insecureAcceptAnything"
                    }
                  ]
                }
              }
            }
          '';
        in
        {
          default = self.packages.${system}.llm-harness;

          containers-policy = localContainersPolicy;

          podman-tool-image = pkgs.callPackage ./nix/podman-tool-image.nix { };

          llm-harness-with-podman-image = pkgs.writeShellApplication {
            name = "llm-harness";
            runtimeInputs = [
              pkgs.podman
              pkgs.skopeo
            ];
            text = ''
              if ! podman image exists "llm-harness-tool:latest" >/dev/null 2>&1; then
                skopeo --policy ${localContainersPolicy} copy \
                  docker-archive:${self.packages.${system}.podman-tool-image} \
                  containers-storage:localhost/llm-harness-tool:latest
              fi

              export HARNESS_PODMAN_IMAGE="''${HARNESS_PODMAN_IMAGE:-llm-harness-tool:latest}"
              export HARNESS_PODMAN_MOUNT_NIX_STORE="''${HARNESS_PODMAN_MOUNT_NIX_STORE:-1}"

              exec ${self.packages.${system}.llm-harness}/bin/llm-harness "$@"
            '';
          };

          llm-harness = pythonPackages.buildPythonApplication {
            pname = "llm-harness";
            version = "0.1.0";
            pyproject = true;
            src = ./.;

            nativeBuildInputs = [
              pythonPackages.hatchling
            ];

            propagatedBuildInputs = with pythonPackages; [
              fastapi
              httpx
              pydantic
              uvicorn
            ];

            nativeCheckInputs = [
              pythonPackages.pytest
            ];

            checkPhase = ''
              runHook preCheck
              pytest
              runHook postCheck
            '';

            pythonImportsCheck = [
              "llm_harness.api"
              "llm_harness.config"
              "llm_harness.core.events"
            ];
          };
        }
      );

      checks = forAllSystems (
        { system, ... }:
        {
          inherit (self.packages.${system}) llm-harness;
        }
      );

      apps = forAllSystems (
        { system, ... }:
        {
          default = {
            type = "app";
            program = "${self.packages.${system}.llm-harness-with-podman-image}/bin/llm-harness";
            meta.description = "Run LLM Harness and load the Nix-built Podman tool image when needed.";
          };
        }
      );

      devShells = forAllSystems (
        { pkgs, python, pythonPackages, ... }:
        {
          default = pkgs.mkShell {
            packages = [
              python
              pythonPackages.fastapi
              pythonPackages.httpx
              pythonPackages.pydantic
              pythonPackages.pytest
              pythonPackages.uvicorn
            ];
          };
        }
      );

      overlays.default = final: prev: {
        llm-harness = self.packages.${final.stdenv.hostPlatform.system}.llm-harness;
      };

      nixosModules.default = import ./nix/nixos-module.nix self;
    };
}
