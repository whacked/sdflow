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
  nix_shortcuts = import (pkgs.fetchurl {
    url = "https://raw.githubusercontent.com/whacked/setup/refs/heads/master/bash/nix_shortcuts.nix.sh";
    hash = "sha256-jLbvJ52h12eug/5Odo04kvHqwOQRzpB9X3bUEB/vzxc=";
  }) { inherit pkgs; };

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
    pkgs.awscli2
    pkgs.check-jsonschema
    pkgs.jsonnet
    pkgs.jq
    pkgs.minio
    pkgs.minio-client
    pkgs.nodejs
    go-jsonschema
  ]
  ++ nix_shortcuts.buildInputs
  ;  # join lists with ++

  shellHook = nix_shortcuts.shellHook + ''
    export MINIO_ROOT_USER=miniotester
    export MINIO_ROOT_PASSWORD=miniotester

    WORKDIR=$PWD
    MINIO_ROOT_DIRECTORY=$WORKDIR/.sdflow.cache/minio

    start-minio-server() {  # start test minio server
      export MC_CONFIG_DIR=$MINIO_ROOT_DIRECTORY/config
      _minio_data_directory=$MINIO_ROOT_DIRECTORY/data
      if [ ! -e "$_minio_data_directory" ]; then
        mkdir -p "$_minio_data_directory"
      fi
      if [ ! -e "$MC_CONFIG_DIR" ]; then
        mkdir -p "$MC_CONFIG_DIR"
      fi
      minio server "$_minio_data_directory"
    }

    initalize-minio-client-account() {
      export MC_CONFIG_DIR=$MINIO_ROOT_DIRECTORY/config
      mc alias set localtest http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
      mc admin user add localtest myaccesskey mysecretkey
      mc admin policy attach localtest readwrite --user myaccesskey
    }

    export PATH=$PWD/result/bin:$PATH
    eval "$(sdflow --completions bash)"
  '' + ''
    alias sdf=sdflow
    complete -F _sdflow_auto_complete sdf
    echo-shortcuts ${__curPos.file}
    unset shellHook
  '';  # join strings with +
}
