{ lib, pkgs }:

let
  imageName = "llm-harness-tool";

  linkTree = pkgs.runCommand "${imageName}-rootfs" { } ''
    mkdir -p "$out/bin" "$out/usr/bin" "$out/tmp" "$out/root"
    chmod 1777 "$out/tmp"

    link_bin() {
      src="$1"
      name="$2"
      ln -s "$src" "$out/bin/$name"
    }

    link_dir() {
      dir="$1"
      for src in "$dir"/*; do
        if [ -x "$src" ] && [ ! -e "$out/bin/$(basename "$src")" ]; then
          ln -s "$src" "$out/bin/$(basename "$src")"
        fi
      done
    }

    link_dir "${pkgs.coreutils}/bin"
    link_dir "${pkgs.findutils}/bin"
    link_dir "${pkgs.gnugrep}/bin"
    link_dir "${pkgs.gnused}/bin"

    rm -f "$out/bin/sh" "$out/bin/bash"
    link_bin "${pkgs.bashInteractive}/bin/bash" bash
    link_bin "${pkgs.bashInteractive}/bin/bash" sh
    ln -s /bin/env "$out/usr/bin/env"
  '';
in
pkgs.dockerTools.buildImage {
  name = imageName;
  tag = "latest";

  copyToRoot = linkTree;

  config = {
    Cmd = [ "/bin/sleep" "infinity" ];
    Env = [
      "PATH=/bin:/usr/bin"
    ];
    WorkingDir = "/root";
  };
}
