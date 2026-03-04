const express = require("express");
const path = require("path");
const fs = require("fs");

const DataSourceFactory = require("./data_source_factory.js");
const cacheManager = require("./cache_manager.js");
const { judgeEncryptSignValid } = require("./request_sign.js");

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
        // 如果有 params 字段，说明是飞书插件调用
        if (body.params) {
            const params =
                typeof body.params === "string"
                    ? JSON.parse(body.params)
                    : body.params;

            // 解析 datasourceConfig
            if (params.datasourceConfig) {
                const config =
                    typeof params.datasourceConfig === "string"
                        ? JSON.parse(params.datasourceConfig)
                        : params.datasourceConfig;

                console.log(
                    "解析后的 datasourceConfig:",
                    JSON.stringify(config).substring(0, 200),
                );
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
app.post("/api/table_meta", async (req, res) => {
    console.log("========== table_meta 请求 ==========");
    console.log("请求体 keys:", Object.keys(req.body));
    console.log("加密判断结果：", judgeEncryptSignValid(req));

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
 */
app.post("/api/records", async (req, res) => {
    console.log("========== records 请求 ==========");
    console.log("请求体 keys:", Object.keys(req.body));
    console.log("加密判断结果：", judgeEncryptSignValid(req));

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

        // 判断数据源类型
        if (config.sqlserver) {
            // SQL Server 数据源
            console.log("→ 使用 SQL Server 数据源");
            data = await getSqlServerTableRecords(
                config.sqlserver,
                config.fields,
            );
        } else {
            // MaxCompute 数据源（默认）
            console.log("→ 使用 MaxCompute 数据源");
            data = await getTableRecords(config);
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
 */
app.post("/api/test_connection", async (req, res) => {
    console.log("test_connection 的请求数据", req.body);

    try {
        // 创建数据源实例
        const dataSource = await DataSourceFactory.createDataSource({ 
            [req.body.dataSourceType]: req.body 
        });
        const result = await dataSource.testConnection();
        await dataSource.close();

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
 */
app.post("/api/test_sqlserver_connection", async (req, res) => {
    console.log("test_sqlserver_connection 的请求数据", req.body);

    try {
        const dataSource = await DataSourceFactory.createDataSource({ sqlserver: req.body });
        const result = await dataSource.testConnection();
        await dataSource.close();

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
 */
app.post("/api/tables", async (req, res) => {
    console.log("tables 的请求数据", req.body);

    try {
        // 尝试从缓存获取
        const cachedTables = cacheManager.getCachedTables(req.body);
        if (cachedTables) {
            console.log("从缓存获取表列表");
            return res.status(200).json({
                code: 0,
                message: "获取表列表成功（缓存）",
                data: cachedTables,
            });
        }

        // 创建数据源实例
        const dataSource = await DataSourceFactory.createDataSource({ 
            [req.body.dataSourceType]: req.body 
        });
        const tables = await dataSource.getTables();
        await dataSource.close();

        // 缓存结果
        cacheManager.cacheTables(req.body, tables);

        res.status(200).json({
            code: 0,
            message: "获取表列表成功",
            data: tables,
        });
    } catch (error) {
        console.error("获取表列表失败:", error);
        res.status(500).json({
            code: 500,
            message: "获取表列表失败: " + error.message,
            data: null,
        });
    }
});

/**
 * 获取 SQL Server 表列表接口（兼容旧版本）
 */
app.post("/api/sqlserver_tables", async (req, res) => {
    console.log("sqlserver_tables 的请求数据", req.body);

    try {
        // 尝试从缓存获取
        const cachedTables = cacheManager.getCachedTables(req.body);
        if (cachedTables) {
            console.log("从缓存获取表列表");
            return res.status(200).json({
                code: 0,
                message: "获取表列表成功（缓存）",
                data: cachedTables,
            });
        }

        const dataSource = await DataSourceFactory.createDataSource({ sqlserver: req.body });
        const tables = await dataSource.getTables();
        await dataSource.close();

        // 缓存结果
        cacheManager.cacheTables(req.body, tables);

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
 */
app.post("/api/maxcompute_tables", async (req, res) => {
    console.log("maxcompute_tables 的请求数据", req.body);

    try {
        // 尝试从缓存获取
        const cachedTables = cacheManager.getCachedTables(req.body);
        if (cachedTables) {
            console.log("从缓存获取表列表");
            return res.status(200).json({
                code: 0,
                message: "获取表列表成功（缓存）",
                data: cachedTables,
            });
        }

        const dataSource = await DataSourceFactory.createDataSource({ maxcompute: req.body });
        const tables = await dataSource.getTables();
        await dataSource.close();

        // 缓存结果
        cacheManager.cacheTables(req.body, tables);

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
