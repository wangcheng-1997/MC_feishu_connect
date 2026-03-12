{ pkgs }:

pkgs.mkShell {
  buildInputs = [
    pkgs.nodejs-20_x
    pkgs.npm
    pkgs.python311
    pkgs.python311Packages.pip
    pkgs.python311Packages.setuptools
    pkgs.python311Packages.wheel
  ];

  shellHook = ''
    export NODE_ENV="production"
    export PORT="3000"
    export PATH="$HOME/.local/bin:$PATH"
    export PYTHONPATH="$HOME/.local/lib/python3.11/site-packages:$PYTHONPATH"
    # 安装 PyODPS 依赖
    python3.11 -m pip install --user --upgrade pip setuptools wheel > /dev/null 2>&1 || true
    python3.11 -m pip install --user pyodps > /dev/null 2>&1 || true
  '';
}