#!/bin/bash
#
# MC Feishu Connect 阿里云一键部署脚本
# Usage: 
#   1. 子域名方式：sudo bash deploy-aliyun.sh <your-domain> <your-secret-key>
#      Example: sudo bash deploy-aliyun.sh feishu.example.com my-secret-key
#   2. 子路径方式：sudo bash deploy-aliyun.sh <your-domain> <sub-path> <your-secret-key>
#      Example: sudo bash deploy-aliyun.sh example.com feishu my-secret-key
#


set -e

# 检查参数
echo ">>> 参数个数: $#"
echo ">>> 参数1: $1"
echo ">>> 参数2: $2"
echo ">>> 参数3: $3"

if [ $# -eq 2 ]; then
    # 子域名方式
    DOMAIN=$1
    SECRET_KEY=$2
    SUBPATH=""
    IS_SUBPATH=0
elif [ $# -eq 3 ]; then
    # 子路径方式
    DOMAIN=$1
    SUBPATH=$2
    SECRET_KEY=$3
    IS_SUBPATH=1
else
    echo "Usage:"
    echo "  1. 子域名方式：sudo bash deploy-aliyun.sh <your-domain> <your-secret-key>"
    echo "     Example: sudo bash deploy-aliyun.sh feishu.example.com my-secret-key"
    echo "  2. 子路径方式：sudo bash deploy-aliyun.sh <your-domain> <sub-path> <your-secret-key>"
    echo "     Example: sudo bash deploy-aliyun.sh example.com feishu my-secret-key"
    exit 1
fi

DEPLOY_DIR=/home/ubuntu/MC_feishu_connect

# 确保 SUBPATH 不以 / 开头和结尾
if [ $IS_SUBPATH -eq 1 ]; then
    SUBPATH_CLEAN=$(echo "$SUBPATH" | sed 's/^[/]*//' | sed 's/[/]*$//')
fi

echo "===================================================="
echo "MC Feishu Connect 一键部署开始"
echo "域名: $DOMAIN"
echo "部署目录: $DEPLOY_DIR"
echo "===================================================="
echo

# 更新系统
echo ">>> 更新系统包..."
apt update && apt upgrade -y

# 安装基础依赖
echo ">>> 安装基础依赖..."
apt install -y nodejs python3 python3-pip nginx certbot python3-certbot-nginx git

# 安装 Node.js 20
echo ">>> 安装 Node.js 20..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs

# 安装 PM2
echo ">>> 安装 PM2..."
npm install pm2 -g

# 安装 PyODPS
echo ">>> 安装 PyODPS..."
pip3 install pyodps

# 克隆代码
echo ">>> 克隆代码..."
if [ ! -d "$DEPLOY_DIR" ]; then
    git clone https://github.com/wangcheng-1997/MC_feishu_connect.git "$DEPLOY_DIR"
fi

cd "$DEPLOY_DIR"

# 拉取最新代码
echo ">>> 拉取最新代码..."
git pull

# 安装 NPM 依赖
echo ">>> 安装 NPM 依赖..."
npm install

# 构建前端
echo ">>> 构建前端..."
npm run build

# 创建 PM2 配置
echo ">>> 创建 PM2 配置..."
cat > ecosystem.config.js << EOF
module.exports = {
  apps: [{
    name: "mc-feishu-connect",
    script: "packages/backend/index.js",
    cwd: "$DEPLOY_DIR",
    env: {
      NODE_ENV: "production",
      PORT: "5000",
      REQUEST_SIGN_SECRET: "$SECRET_KEY"
    }
  }]
}
EOF

# 启动服务
echo ">>> 启动服务..."
pm2 start ecosystem.config.js
pm2 save
pm2 startup

# 创建 Nginx 配置（只保留 HTTP，让 Certbot 验证）
echo ">>> 创建 Nginx 配置..."
cat > /etc/nginx/sites-available/feishu-connector << EOF
server {
    listen 80;
    server_name $DOMAIN;

    # Let's Encrypt 验证
    location ~ /.well-known/acme-challenge {
        allow all;
        root /var/www/html;
    }

EOF

# 根据是否子路径添加不同 location
if [ $IS_SUBPATH -eq 1 ]; then
cat >> /etc/nginx/sites-available/feishu-connector << EOF
    # 所有 /$SUBPATH_CLEAN 开头的请求代理到后端
    location ~ ^/$SUBPATH_CLEAN(/.*)\$ {
        proxy_pass http://127.0.0.1:5000\$1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout 60s;
        proxy_read_timeout 60s;
    }
    # 处理不带斜杠的根路径，跳转到带斜杠
    location ^~ /$SUBPATH_CLEAN\$ {
        return 301 /$SUBPATH_CLEAN/;
    }
    # 处理 /$SUBPATH_CLEAN/ 根路径
    location = /$SUBPATH_CLEAN/ {
        proxy_pass http://127.0.0.1:5000/;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout 60s;
        proxy_read_timeout 60s;
    }
}
EOF
else
cat >> /etc/nginx/sites-available/feishu-connector << EOF
    # 根路径所有请求代理到后端
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout 60s;
        proxy_read_timeout 60s;
    }
}
EOF
fi

# 启用配置
echo ">>> 启用 Nginx 配置..."
ln -sf /etc/nginx/sites-available/feishu-connector /etc/nginx/sites-enabled/
if [ $IS_SUBPATH -ne 1 ]; then
    rm -f /etc/nginx/sites-enabled/default
fi
nginx -t
systemctl reload nginx

# 申请 SSL 证书（Certbot 会自动找到 Nginx 配置并添加 HTTPS）
echo ">>> 申请 SSL 证书..."
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m 18130304002@163.com

# 重启 Nginx
echo ">>> 重启 Nginx..."
systemctl reload nginx

echo
echo "===================================================="
echo "✅ 部署完成！"
echo "===================================================="
echo
echo "服务信息:"
if [ $IS_SUBPATH -eq 1 ]; then
echo "  访问地址: https://$DOMAIN/$SUBPATH_CLEAN"
echo "  健康检查: https://$DOMAIN/$SUBPATH_CLEAN/health"
else
echo "  域名: https://$DOMAIN"
echo "  健康检查: https://$DOMAIN/health"
fi
echo "  PM2 状态: pm2 status"
echo "  PM2 日志: pm2 logs mc-feishu-connect"
echo
echo "飞书配置:"
if [ $IS_SUBPATH -eq 1 ]; then
echo "  服务地址: https://$DOMAIN/$SUBPATH_CLEAN"
echo "  Verification Token: $SECRET_KEY"
echo "  dataSourceConfigUiUri: /$SUBPATH_CLEAN/"
echo "  tableMeta uri: /$SUBPATH_CLEAN/api/table_meta"
echo "  records uri: /$SUBPATH_CLEAN/api/records"
else
echo "  服务地址: https://$DOMAIN"
echo "  Verification Token: $SECRET_KEY"
echo "  dataSourceConfigUiUri: /"
echo "  tableMeta uri: /api/table_meta"
echo "  records uri: /api/records"
fi
echo
echo "===================================================="
