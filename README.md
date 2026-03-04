# MC Feishu Connect

飞书连接器 - 用于 MaxCompute 和 SQL Server 数据同步的完整解决方案。

## 项目结构

```
MC_feishu_connect/
├── packages/
│   ├── frontend/          # React + TypeScript 前端
│   │   ├── src/           # 源代码
│   │   ├── public/        # 静态资源
│   │   └── package.json   # 前端依赖
│   └── backend/           # Node.js + Express 后端
│       ├── index.js       # 入口文件
│       ├── public/        # 静态资源
│       └── package.json   # 后端依赖
├── package.json           # 根目录配置
└── README.md              # 项目说明
```

## 技术栈

### 前端
- React 18 + TypeScript
- Vite (构建工具)
- Ant Design (UI组件库)
- @lark-base-open/connector-api (飞书连接器API)

### 后端
- Node.js + Express
- MSSQL (SQL Server 连接)
- ODBC (数据库连接)
- Axios (HTTP 请求)

## 快速开始

### 安装依赖

```bash
npm run install:all
```

### 开发模式

同时启动前端和后端：

```bash
# 终端 1 - 启动后端
npm run dev:backend

# 终端 2 - 启动前端
npm run dev:frontend
```

### 构建前端

```bash
npm run build
```

### 生产部署

```bash
npm start
```

## 功能特性

- MaxCompute 数据表元数据获取
- MaxCompute 数据记录查询
- SQL Server 数据表元数据获取
- SQL Server 数据记录查询
- 请求签名验证
- 跨域支持

## API 端点

- `GET /` - 服务状态检查
- `GET /meta.json` - 获取连接器配置
- `POST /meta` - 获取表元数据
- `POST /records` - 获取表记录
- `POST /sqlserver/meta` - 获取 SQL Server 表元数据
- `POST /sqlserver/records` - 获取 SQL Server 表记录

## 许可证

ISC
