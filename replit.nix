{ pkgs }:

pkgs.mkShell {
  buildInputs = [
    pkgs.nodejs-20_x
    pkgs.npm
    pkgs.python311
    pkgs.python311Packages.pip
  ];

  shellHook = ''
    export NODE_ENV="production"
    export PORT="3000"
    export PATH="$HOME/.local/bin:$PATH"
    # 安装 PyODPS 依赖
    pip install --user pyodps > /dev/null 2>&1 || true
  '';
}