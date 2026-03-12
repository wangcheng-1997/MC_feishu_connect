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

### 前置要求

- Node.js >= 20.0.0
- Python >= 3.6
- PyODPS: `pip install pyodps`

### 安装依赖

```bash
npm run install:all
pip install pyodps
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

## MaxCompute 配置说明

> **注意**：项目使用官方 Python SDK (PyODPS) 进行连接，请确保已安装：`pip install pyodps`

### Endpoint 配置

MaxCompute Endpoint 是访问 MaxCompute 服务的接入点。根据官方文档，支持三种网络类型：

#### 1. 公网 Endpoint
适用于从阿里云外部（如办公电脑）访问 MaxCompute。

示例：
- 华东1（杭州）: `https://service.cn-hangzhou.maxcompute.aliyun.com/api`
- 华东2（上海）: `https://service.cn-shanghai.maxcompute.aliyun.com/api`
- 华北2（北京）: `https://service.cn-beijing.maxcompute.aliyun.com/api`

#### 2. VPC Endpoint
适用于从阿里云内部（如 ECS 实例）访问 MaxCompute，更安全且稳定。

示例：
- 华东1（杭州）: `https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api`
- 华东2（上海）: `https://service.cn-shanghai-vpc.maxcompute.aliyun-inc.com/api`

#### 3. 云产品互联 Endpoint
适用于从阿里云产品互联网络访问 MaxCompute（如从 Quick BI 连接）。

示例：
- 华东1（杭州）: `https://service.cn-hangzhou-intranet.maxcompute.aliyun-inc.com/api`

### 使用方式

```javascript
const { MaxComputeClient, getEndpoint } = require('./maxcompute_client.js');

// 方式一：直接指定 Endpoint
const client = new MaxComputeClient({
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  endpoint: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api',
  projectName: 'your-project-name',
  schemaName: 'default'
});

// 方式二：使用区域和网络类型自动生成 Endpoint
const endpoint = getEndpoint('cn-hangzhou', 'public'); // 或 'vpc', 'intranet'

const client = new MaxComputeClient({
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  endpoint: endpoint,
  projectName: 'your-project-name',
  schemaName: 'default'
});
```

### 支持的区域

- 中国区域：cn-hangzhou, cn-shanghai, cn-beijing, cn-zhangjiakou, cn-wulanchabu, cn-shenzhen, cn-chengdu, cn-hongkong
- 亚太区域：ap-southeast-1 (新加坡), ap-northeast-1 (东京)
- 欧洲区域：eu-central-1 (法兰克福)
- 美洲区域：us-west-1 (硅谷), us-east-1 (弗吉尼亚)

更多详细信息请参考官方文档：https://help.aliyun.com/zh/maxcompute/user-guide/endpoints

## 故障排查

常见问题：
- **连接失败**：检查 Python 和 PyODPS 是否正确安装，运行 `pip show pyodps`
- **权限错误**：检查 AccessKey 权限是否足够
- **项目不存在**：检查项目名称和 Endpoint 区域是否正确
- **连接超时**：检查网络连接和防火墙设置

### 测试 PyODPS

```bash
python -c "from odps import ODPS; print('PyODPS OK')"
```

### 官方文档

- MaxCompute 官方文档：https://help.aliyun.com/zh/maxcompute/
- PyODPS 文档：https://help.aliyun.com/zh/maxcompute/user-guide/faq-about-pyodps

## API 端点

- `GET /` - 服务状态检查
- `GET /meta.json` - 获取连接器配置
- `POST /meta` - 获取表元数据
- `POST /records` - 获取表记录
- `POST /sqlserver/meta` - 获取 SQL Server 表元数据
- `POST /sqlserver/records` - 获取 SQL Server 表记录

## 许可证

ISC
