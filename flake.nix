{
  nixConfig.bash-prompt = ''\033[1;32m\[[nix-develop:\[\033[36m\]\w\[\033[32m\]]$\033[0m '';

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.gomod2nix.url = "github:nix-community/gomod2nix";
  inputs.gomod2nix.inputs.nixpkgs.follows = "nixpkgs";
  inputs.gomod2nix.inputs.flake-utils.follows = "flake-utils";

  outputs = { self, nixpkgs, flake-utils, gomod2nix }:
    (flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};

          # The current default sdk for macOS fails to compile go projects, so we use a newer one for now.
          # This has no effect on other platforms.
          callPackage = pkgs.darwin.apple_sdk_12_0.callPackage or pkgs.callPackage;
        in
        let
          sdflow = callPackage ./. {
            inherit (gomod2nix.legacyPackages.${system}) buildGoApplication;
          };
        in
        {
          packages = {
            default = sdflow;
            sdflow = sdflow;
          };

          apps = {
            default = {  # change to `sdflow = ...` if you want to use `nix build .#sdflow`
              type = "app";
              program = "${sdflow}/bin/sdflow";
            };
          };

          devShells.default = callPackage ./shell.nix {
            inherit (gomod2nix.legacyPackages.${system}) mkGoEnv gomod2nix;
          };
        })
    );
}
