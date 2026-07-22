{
  description = "Session-oriented LLM harness with plugin providers, tools, database hooks, and streaming.";

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
          f {
            inherit system;
            pkgs = import nixpkgs { inherit system; };
          }
        );
    in
    {
      packages = forAllSystems (
        { pkgs, system }:
        {
          default = self.packages.${system}.llm-harness;

          podman-tool-image = pkgs.callPackage ./nix/podman-tool-image.nix { };

          llm-harness = pkgs.python312Packages.buildPythonApplication {
            pname = "llm-harness";
            version = "0.1.0";
            pyproject = true;
            src = ./.;

            nativeBuildInputs = [
              pkgs.python312Packages.hatchling
            ];

            propagatedBuildInputs = with pkgs.python312Packages; [
              fastapi
              httpx
              pydantic
              uvicorn
            ];

            nativeCheckInputs = [
              pkgs.python312Packages.pytest
            ];

            checkPhase = ''
              runHook preCheck
              pytest
              runHook postCheck
            '';

            pythonImportsCheck = [
              "llm_harness.api"
              "llm_harness.config"
              "llm_harness.db"
            ];
          };
        }
      );

      checks = forAllSystems (
        { pkgs, system }:
        {
          inherit (self.packages.${system}) llm-harness;
        }
      );

      devShells = forAllSystems (
        { pkgs, ... }:
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.python312
              pkgs.python312Packages.fastapi
              pkgs.python312Packages.httpx
              pkgs.python312Packages.pydantic
              pkgs.python312Packages.pytest
              pkgs.python312Packages.uvicorn
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
