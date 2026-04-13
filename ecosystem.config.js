module.exports = {
  apps: [{
    name: "mc-feishu-connect",
    script: "packages/backend/index.js",
    cwd: "/home/ubuntu/MC_feishu_connect",
    env: {
      NODE_ENV: "production",
      PORT: "5000",
      REQUEST_SIGN_SECRET: process.env.REQUEST_SIGN_SECRET
    },
    // 日志配置
    log_date_format: "YYYY-MM-DD HH:mm:ss",
    error_file: "/var/log/mc-feishu-connect/error.log",
    // 不记录 stdout（数据输出）
    out_file: "/dev/null",
    merge_logs: false,
    // 进程管理
    max_restarts: 10,
    min_uptime: "10s",
    max_memory_restart: "500M",
    watch: false,
    autorestart: true
  }]
};
