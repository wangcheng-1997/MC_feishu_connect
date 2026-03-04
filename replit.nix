{ pkgs }:

pkgs.mkShell {
  buildInputs = [
    pkgs.nodejs-20_x
    pkgs.npm
  ];

  shellHook = ''
    export NODE_ENV="production"
    export PORT="3000"
  '';
}