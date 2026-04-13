const express = require("express");
const path = require("path");
const fs = require("fs");

const DataSourceFactory = require("./data_source_factory.js");
const { judgeEncryptSignValid, setSecretKey } = require("./request_sign.js");
const { getTableMeta } = require("./table_meta.js");
const { getTableRecords } = require("./table_records.js");
const {
    getSqlServerTableMeta,
    getSqlServerTableRecords,
} = require("./sqlserver_handler.js");

// 从环境变量读取请求签名秘钥
if (process.env.REQUEST_SIGN_SECRET) {
    setSecretKey(process.env.REQUEST_SIGN_SECRET);
    console.log("已加载请求签名秘钥");
} else {
    console.log("未设置 REQUEST_SIGN_SECRET 环境变量，跳过签名验证");
}

/**
 * 请求签名验证中间件
 * 验证飞书请求签名，验证失败直接返回 401
 */
function validateRequestSignature(req, res, next) {
    try {
        // 生产环境不打印详细请求信息
        // console.log("========== 收到请求 ==========");
        // console.log("方法:", req.method);
        // console.log("路径:", req.path);
        // console.log("Headers:", JSON.stringify(req.headers, null, 2));
        // console.log("Body:", JSON.stringify(req.body, null, 2));
        // console.log("==============================");
        
        const isValid = judgeEncryptSignValid(req);
        
        if (!isValid) {
            return res.status(401).json({ code: 401, message: '签名验证失败' });
        }
        
        next();
    } catch (error) {
        console.error('签名验证错误:', error.message);
        return res.status(500).json({ code: 500, message: '签名验证服务异常' });
    }
}

const app = express();

// 跨域支持
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header(
        "Access-Control-Allow-Methods",
        "GET, POST, PUT, DELETE, OPTIONS",
    );
    res.header(
        "Access-Control-Allow-Headers",
        "Origin, X-Requested-With, Content-Type, Accept, Authorization",
    );

    if (req.method === "OPTIONS") {
        res.sendStatus(200);
    } else {
        next();
    }
});

app.use(express.json());

// 静态文件服务
app.use(express.static(path.join(__dirname, "../frontend/dist")));

app.get("*", (req, res, next) => {
    // 如果是 API 请求，跳过
    if (req.path.startsWith("/api") || req.path === "/health" || req.path === "/meta.json") {
        return next();
    }
    const indexPath = path.join(__dirname, "../frontend/dist/index.html");
    if (fs.existsSync(indexPath)) {
        res.sendFile(indexPath);
    } else {
        res.send("Data Sync Connector (MaxCompute & SQL Server) - Running (Frontend not built yet)");
    }
});

app.get("/meta.json", (req, res) => {
    fs.readFile(
        path.join(__dirname, "./public/meta.json"),
        "utf8",
        (err, data) => {
            if (err) {
                res.status(500).json({ code: 500, message: "读取配置失败" });
                return;
            }
            res.set("Content-Type", "application/json");
            res.status(200).send(data);
        },
    );
});

/**
 * 解析飞书插件传递的参数
 * 飞书插件会将配置封装在 params.datasourceConfig 中
 */
function parseLarkParams(body) {
    try {
        if (body.params) {
            const params =
                typeof body.params === "string"
                    ? JSON.parse(body.params)
                    : body.params;

            if (params.datasourceConfig) {
                const config =
                    typeof params.datasourceConfig === "string"
                        ? JSON.parse(params.datasourceConfig)
                        : params.datasourceConfig;

                console.log(
                    "解析后的 datasourceConfig:",
                    JSON.stringify(config).substring(0, 200),
                );
                
                if (params.pageToken) {
                    config.pageToken = params.pageToken;
                }
                if (params.nextPageToken) {
                    config.nextPageToken = params.nextPageToken;
                }
                
                return config;
            }
        }
    } catch (error) {
        console.error("解析参数失败:", error);
    }
    return null;
}

/**
 * 获取表元数据接口
 * 支持 MaxCompute 和 SQL Server
 */
app.post("/api/table_meta", validateRequestSignature, async (req, res) => {
    console.log("========== table_meta 请求 ==========");
    console.log("请求体 keys:", Object.keys(req.body));

    try {
        let data;

        // 首先检查是否有直接的数据源配置
        let config = req.body;

        // 如果没有，尝试解析飞书插件的参数
        if (!config.sqlserver && !config.maxcompute) {
            const parsedConfig = parseLarkParams(req.body);
            if (parsedConfig) {
                config = parsedConfig;
            }
        }

        console.log(
            "最终使用的配置:",
            JSON.stringify(config).substring(0, 300),
        );

        // 判断数据源类型
        if (config.sqlserver) {
            // SQL Server 数据源
            console.log("→ 使用 SQL Server 数据源 (sqlserver_handler.js)");
            data = await getSqlServerTableMeta(config.sqlserver);
        } else if (config.maxcompute) {
            // MaxCompute 数据源
            console.log("→ 使用 MaxCompute 数据源 (table_meta.js)");
            data = await getTableMeta(config);
        } else {
            // 没有明确的数据源，返回默认
            console.log("→ 无数据源配置，返回默认");
            data = await getTableMeta(config);
        }

        const result = {
            code: 0,
            message: "获取表元数据成功",
            data: data,
        };
        res.status(200).json(result);
    } catch (error) {
        console.error("获取表元数据失败:", error);
        res.status(500).json({
            code: 500,
            message: "获取表元数据失败: " + error.message,
            data: null,
        });
    }
});

/**
 * 获取表记录数据接口
 * 支持 MaxCompute 和 SQL Server
 * 支持分批写入（每批最多1000条记录）
 */
app.post("/api/records", validateRequestSignature, async (req, res) => {
    try {
        let data;

        // 首先检查是否有直接的数据源配置
        let config = req.body;

        // 如果没有，尝试解析飞书插件的参数
        if (!config.sqlserver && !config.maxcompute) {
            const parsedConfig = parseLarkParams(req.body);
            if (parsedConfig) {
                config = parsedConfig;
            }
        }

        // 获取分页参数（用于分批写入）
        // 飞书多维表格会使用 pageToken 作为下一次请求的 offset 参数
        let offset = parseInt(config.offset) || 0;
        // limit 优先级：顶层 limit > maxcompute.limit/sqlserver.limit > 默认 1000
        let limit = parseInt(config.limit) || parseInt(config.maxcompute?.limit) || parseInt(config.sqlserver?.limit) || 1000;
        limit = Math.min(limit, 1000); // 最大 1000
        
        console.log(`[limit解析] config.limit=${config.limit}, maxcompute.limit=${config.maxcompute?.limit}, 最终limit=${limit}`);
        
        // 如果有 pageToken 或 nextPageToken，则使用它作为 offset
        // 支持两种参数名以确保兼容性
        if (config.pageToken) {
            offset = parseInt(config.pageToken) || 0;
        } else if (config.nextPageToken) {
            offset = parseInt(config.nextPageToken) || 0;
        }

        // 判断数据源类型
        if (config.sqlserver) {
            // SQL Server 数据源
            console.log(`[分页参数] offset=${offset}, limit=${limit}`);
            data = await getSqlServerTableRecords(
                config.sqlserver,
                config.fields,
                offset,
                limit
            );
        } else {
            // MaxCompute 数据源（默认）
            // 构建配置，确保分页参数正确传递
            const configWithPaging = {
                maxcompute: config.maxcompute,
                fields: config.fields,
                offset: offset,
                limit: limit
            };
            console.log(`[分页参数] offset=${offset}, limit=${limit}, sql=${config.maxcompute?.sql?.substring(0, 50)}`);
            data = await getTableRecords(configWithPaging);
        }

        const result = {
            code: 0,
            message: "获取记录数据成功",
            data: data,
        };
        res.status(200).json(result);
    } catch (error) {
        console.error("获取记录数据失败:", error);
        res.status(500).json({
            code: 500,
            message: "获取记录数据失败: " + error.message,
            data: null,
        });
    }
});

/**
 * 健康检查接口
 */
app.get("/health", (req, res) => {
    res.status(200).json({
        code: 0,
        message: "服务运行正常",
        data: {
            status: "healthy",
            timestamp: new Date().toISOString(),
            supportedSources: ["maxcompute", "sqlserver"],
        },
    });
});

/**
 * 测试连接接口
 * 支持 MaxCompute 和 SQL Server
 * 前端配置页面调用，不需要签名验证
 */
app.post("/api/test_connection", async (req, res) => {
    console.log("test_connection 的请求数据", req.body);

    try {
        // 正确处理请求数据格式
        const dataSourceConfig = {};
        dataSourceConfig[req.body.dataSourceType] = req.body;
        
        // 创建数据源实例
        const dataSource = await DataSourceFactory.createDataSource(dataSourceConfig);
        const result = await dataSource.testConnection();
        
        // 安全调用 close 方法
        if (dataSource.close && typeof dataSource.close === 'function') {
            await dataSource.close();
        }

        res.status(200).json({
            code: result.success ? 0 : 500,
            message: result.message,
            data: result,
        });
    } catch (error) {
        console.error("连接测试失败:", error);
        res.status(500).json({
            code: 500,
            message: "连接测试失败: " + error.message,
            data: { success: false },
        });
    }
});



/**
 * 测试 SQL Server 连接接口（兼容旧版本）
 * 前端配置页面调用，不需要签名验证
 */
app.post("/api/test_sqlserver_connection", async (req, res) => {
    console.log("test_sqlserver_connection 的请求数据", req.body);

    try {
        const dataSource = await DataSourceFactory.createDataSource({ sqlserver: req.body });
        const result = await dataSource.testConnection();
        
        // 安全调用 close 方法
        if (dataSource.close && typeof dataSource.close === 'function') {
            await dataSource.close();
        }

        res.status(200).json({
            code: result.success ? 0 : 500,
            message: result.message,
            data: result,
        });
    } catch (error) {
        console.error("SQL Server 连接测试失败:", error);
        res.status(500).json({
            code: 500,
            message: "连接测试失败: " + error.message,
            data: { success: false },
        });
    }
});

/**
 * 获取表列表接口
 * 支持 MaxCompute 和 SQL Server
 * 前端配置页面调用，不需要签名验证
 */
app.post("/api/tables", async (req, res) => {
    console.log("tables 的请求数据", req.body);

    try {
        const dataSourceConfig = {};
        dataSourceConfig[req.body.dataSourceType] = req.body;
        
        const dataSource = await DataSourceFactory.createDataSource(dataSourceConfig);
        const tables = await dataSource.getTables();
        
        if (dataSource.close && typeof dataSource.close === 'function') {
            await dataSource.close();
        }

        res.status(200).json({
            code: 0,
            message: "获取表列表成功",
            data: tables,
        });
    } catch (error) {
        console.error("获取表列表失败:", error);
        res.status(200).json({
            code: 0,
            message: "获取表列表成功",
            data: [],
        });
    }
});

/**
 * 获取 SQL Server 表列表接口（兼容旧版本）
 * 前端配置页面调用，不需要签名验证
 */
app.post("/api/sqlserver_tables", async (req, res) => {
    console.log("sqlserver_tables 的请求数据", req.body);

    try {
        const dataSource = await DataSourceFactory.createDataSource({ sqlserver: req.body });
        const tables = await dataSource.getTables();
        await dataSource.close();

        res.status(200).json({
            code: 0,
            message: "获取表列表成功",
            data: tables,
        });
    } catch (error) {
        console.error("获取 SQL Server 表列表失败:", error);
        res.status(500).json({
            code: 500,
            message: "获取表列表失败: " + error.message,
            data: null,
        });
    }
});

/**
 * 获取 MaxCompute 表列表接口（兼容旧版本）
 * 前端配置页面调用，不需要签名验证
 */
app.post("/api/maxcompute_tables", async (req, res) => {
    console.log("maxcompute_tables 的请求数据", req.body);

    try {
        const dataSource = await DataSourceFactory.createDataSource({ maxcompute: req.body });
        const tables = await dataSource.getTables();
        await dataSource.close();

        res.status(200).json({
            code: 0,
            message: "获取表列表成功",
            data: tables,
        });
    } catch (error) {
        console.error("获取 MaxCompute 表列表失败:", error);
        res.status(500).json({
            code: 500,
            message: "获取表列表失败: " + error.message,
            data: null,
        });
    }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Data Sync Server running on port ${PORT}`);
    console.log(`Supported data sources: MaxCompute, SQL Server`);
    console.log(`Health check: http://localhost:${PORT}/health`);
});
