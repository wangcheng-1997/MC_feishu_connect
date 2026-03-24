#!/bin/bash
#
# MC Feishu Connect 阿里云一键部署脚本
# Usage: sudo bash deploy-aliyun.sh <your-domain> <your-secret-key>
# Example: sudo bash deploy-aliyun.sh connector.example.com my-secret-key
#

set -e

# 检查参数
if [ $# -ne 2 ]; then
    echo "Usage: sudo bash deploy-aliyun.sh <your-domain> <your-secret-key>"
    echo "Example: sudo bash deploy-aliyun.sh connector.example.com my-secret-key"
    exit 1
fi

DOMAIN=$1
SECRET_KEY=$2
DEPLOY_DIR=/home/ubuntu/MC_feishu_connect

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
    git clone https://github.com/KianWang/MC_feishu_connect.git "$DEPLOY_DIR"
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

# 创建 Nginx 配置
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

    location / {
        return 301 https://\$server_name\$request_uri;
    }
}

server {
    listen 443 ssl http2;
    server_name $DOMAIN;

    # SSL 证书位置（Certbot 会自动修改这里）
    ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;

    # SSL 配置
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # 反向代理到本地 Node.js 服务
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

# 启用配置
echo ">>> 启用 Nginx 配置..."
ln -sf /etc/nginx/sites-available/feishu-connector /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx

# 申请 SSL 证书
echo ">>> 申请 SSL 证书..."
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email

# 重启 Nginx
echo ">>> 重启 Nginx..."
systemctl reload nginx

echo
echo "===================================================="
echo "✅ 部署完成！"
echo "===================================================="
echo
echo "服务信息:"
echo "  域名: https://$DOMAIN"
echo "  健康检查: https://$DOMAIN/health"
echo "  PM2 状态: pm2 status"
echo "  PM2 日志: pm2 logs mc-feishu-connect"
echo
echo "飞书配置:"
echo "  服务地址: https://$DOMAIN"
echo "  Verification Token: $SECRET_KEY"
echo "  dataSourceConfigUiUri: /"
echo "  tableMeta uri: /api/table_meta"
echo "  records uri: /api/records"
echo
echo "===================================================="
