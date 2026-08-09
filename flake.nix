{
  description = "Development environment for the Petkit BLE Home Assistant integration";

  inputs.nixpkgs.url = "https://flakehub.com/f/DeterminateSystems/nixpkgs-weekly/*.tar.gz";

  outputs = { nixpkgs, ... }:
    let
      supportedSystems = [ "aarch64-darwin" "aarch64-linux" "x86_64-darwin" "x86_64-linux" ];
      forEachSystem = nixpkgs.lib.genAttrs supportedSystems;
    in {
      devShells = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              git
              python313
              ruff
              uv
            ];
          };
        });
    };
}
