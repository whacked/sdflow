{ pkgs ? (
    let
      inherit (builtins) fetchTree fromJSON readFile;
      inherit ((fromJSON (readFile ./flake.lock)).nodes) nixpkgs gomod2nix;
    in
    import (fetchTree nixpkgs.locked) {
      overlays = [
        (import "${fetchTree gomod2nix.locked}/overlay.nix")
      ];
    }
  )
, mkGoEnv ? pkgs.mkGoEnv
, gomod2nix ? pkgs.gomod2nix
}:

let
  goEnv = mkGoEnv { pwd = ./.; };
  go-jsonschema = pkgs.stdenv.mkDerivation {
    name = "go-jsonschema";
    src = (
      if pkgs.stdenv.hostPlatform.system == "x86_64-linux" then
        pkgs.fetchurl {
          url = "https://github.com/omissis/go-jsonschema/releases/download/v0.15.0/go-jsonschema_Linux_x86_64.tar.gz";
          sha256 = "diR8EUGrEcVyhW5kAyDyHluoWRnj3lUlNL2BbhUjFS4=";
        }
      else if pkgs.stdenv.hostPlatform.system == "aarch64-darwin" then
        pkgs.fetchurl {
          url = "https://github.com/omissis/go-jsonschema/releases/download/v0.19.0/go-jsonschema_Darwin_arm64.tar.gz";
          sha256 = "sha256-Wyke21qZXNRPvwBEMQ/540+snLwdNlKvX0ae2xIshmE=";
        }
      else if pkgs.stdenv.hostPlatform.system == "x86_64-darwin" then
        pkgs.fetchurl {
          url = "https://github.com/omissis/go-jsonschema/releases/download/v0.19.0/go-jsonschema_Darwin_x86_64.tar.gz";
          sha256 = "";
        }
      else
        throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}"
    );

    dontUnpack = true;
    installPhase = ''
      mkdir -p $out/bin
      tar -xzf $src -C $out/bin
    '';
    buildInputs = [ pkgs.unzip ];
  };
in
pkgs.mkShell {
  packages = [
    pkgs.gopls
    goEnv
    gomod2nix
    pkgs.check-jsonschema
    pkgs.jsonnet
    pkgs.jq
    go-jsonschema
  ];

  shellHook = ''
    export PATH=$PWD/result/bin:$PATH
    eval "$(sdflow --completions bash)"
  '';
}
