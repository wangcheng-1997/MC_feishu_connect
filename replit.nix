{ pkgs }:

pkgs.mkShell {
  # 系统依赖
  buildInputs = [
    pkgs.nodejs-18_x
    pkgs.npm
  ];

  # 环境变量
  shellHook = ''
    export NODE_ENV="production"
    export PORT="8080"
  '';
}