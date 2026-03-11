import "./App.css";
import { useState, useEffect } from "react";
import { bitable } from "@lark-base-open/connector-api";
import {
    Button,
    Form,
    Input,
    Select,
    message,
    Card,
    Divider,
    Typography,
    Radio,
    Switch,
    InputNumber,
} from "antd";

const { Title, Text } = Typography;
const { TextArea } = Input;

// 数据源类型
type DataSourceType = "maxcompute" | "sqlserver";

// MaxCompute 配置接口
interface MaxComputeConfig {
    accessId: string;
    accessKey: string;
    endpoint: string;
    projectName: string;
    schemaName: string;
    tableName: string;
    primaryField: string;
    sql: string;
    limit: number;
}

// SQL Server 配置接口
interface SqlServerConfig {
    server: string;
    port: number;
    database: string;
    user: string;
    password: string;
    schema: string;
    tableName: string;
    primaryField: string;
    sql: string;
    limit: number;
    encrypt: boolean;
    trustServerCertificate: boolean;
}

const ENDPOINT_OPTIONS = [
    {
        value: "http://service.cn.maxcompute.aliyun.com/api",
        label: "阿里云中国站 (cn)",
    },
    {
        value: "http://service.cn-beijing.maxcompute.aliyun.com/api",
        label: "华北2 (北京)",
    },
    {
        value: "http://service.cn-shanghai.maxcompute.aliyun.com/api",
        label: "华东2 (上海)",
    },
    {
        value: "http://service.cn-hangzhou.maxcompute.aliyun.com/api",
        label: "华东1 (杭州)",
    },
    {
        value: "http://service.cn-shenzhen.maxcompute.aliyun.com/api",
        label: "华南1 (深圳)",
    },
    {
        value: "http://service.cn-qingdao.maxcompute.aliyun.com/api",
        label: "华北1 (青岛)",
    },
    {
        value: "http://service.cn-zhangjiakou.maxcompute.aliyun.com/api",
        label: "华北3 (张家口)",
    },
    {
        value: "http://service.cn-huhehaote.maxcompute.aliyun.com/api",
        label: "华北5 (呼和浩特)",
    },
    {
        value: "http://service.cn-chengdu.maxcompute.aliyun.com/api",
        label: "西南1 (成都)",
    },
    {
        value: "http://service.cn-hongkong.maxcompute.aliyun.com/api",
        label: "香港",
    },
    {
        value: "http://service.ap-southeast-1.maxcompute.aliyun.com/api",
        label: "新加坡",
    },
    {
        value: "http://service.us-west-1.maxcompute.aliyun.com/api",
        label: "美国 (硅谷)",
    },
    {
        value: "http://service.us-east-1.maxcompute.aliyun.com/api",
        label: "美国 (弗吉尼亚)",
    },
    {
        value: "http://service.eu-central-1.maxcompute.aliyun.com/api",
        label: "德国 (法兰克福)",
    },
];



export default function App() {
    const [form] = Form.useForm();
    const [loading, setLoading] = useState(false);
    const [userId, setUserId] = useState("");
    const [tenantKey, setTenantKey] = useState("");
    const [dataSourceType, setDataSourceType] = useState<DataSourceType>("maxcompute");

    useEffect(() => {
        // 加载已保存的配置
        bitable.getConfig().then((config: any) => {
            console.log("加载已保存的配置", config);
            if (config?.maxcompute) {
                setDataSourceType("maxcompute");
                form.setFieldsValue(config.maxcompute);
            } else if (config?.sqlserver) {
                setDataSourceType("sqlserver");
                form.setFieldsValue(config.sqlserver);
            }
        });

        // 获取用户信息
        bitable.getUserId().then((id) => {
            console.log("userId", id);
            setUserId(id);
        });

        bitable.getTenantKey().then((key) => {
            console.log("tenantKey", key);
            setTenantKey(key);
        });
    }, [form]);

    const handleDataSourceChange = (e: any) => {
        setDataSourceType(e.target.value);
        form.resetFields();
    };





    const handleSaveConfig = async (values: any) => {
        setLoading(true);
        try {
            let config: any = {};

            if (dataSourceType === "maxcompute") {
                config = {
                    maxcompute: {
                        accessId: values.accessId,
                        accessKey: values.accessKey,
                        endpoint: values.endpoint,
                        projectName: values.projectName,
                        schemaName: values.schemaName || "default",
                        tableName: values.tableName,
                        primaryField: values.primaryField,
                        sql: values.sql || "",
                        limit: values.limit || 1000,
                    },
                };
            } else {
                config = {
                    sqlserver: {
                        server: values.server,
                        port: parseInt(values.port, 10) || 1433,
                        database: values.database,
                        user: values.user,
                        password: values.password,
                        schema: values.schema || "dbo",
                        tableName: values.tableName,
                        primaryField: values.primaryField,
                        sql: values.sql || "",
                        limit: values.limit || 1000,
                        encrypt: values.encrypt !== false,
                        trustServerCertificate: values.trustServerCertificate === true,
                    },
                };
            }

            console.log("保存配置", config);

            // 保存配置并进入下一步
            bitable.saveConfigAndGoNext(config);
            message.success("配置保存成功");
        } catch (error) {
            console.error("保存配置失败:", error);
            message.error("保存配置失败");
        } finally {
            setLoading(false);
        }
    };

    const [testingConnection, setTestingConnection] = useState(false);
    const [loadingTables, setLoadingTables] = useState(false);
    const [tables, setTables] = useState<any[]>([]);
    const [tableSearchValue, setTableSearchValue] = useState('');

    const handleTestConnection = async () => {
        try {
            // 只验证必要的连接字段，不验证表名
            const values = await form.validateFields([
                dataSourceType === "maxcompute" 
                    ? ['accessId', 'accessKey', 'endpoint', 'projectName', 'schemaName']
                    : ['server', 'port', 'database', 'user', 'password', 'schema', 'encrypt', 'trustServerCertificate']
            ]);
            setTestingConnection(true);
            
            // 后端服务地址，使用相对路径（部署时自动适应）
            const baseUrl = '';
            const testUrl = `${baseUrl}/api/test_connection`;
            
            // 构建请求体，包含数据源类型
            const requestBody = {
                dataSourceType,
                ...values
            };
            
            const response = await fetch(testUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(requestBody),
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.code === 0 && result.data?.success) {
                message.success(result.message || "连接成功！");
                // 连接成功后自动获取表列表
                handleGetTables();
            } else {
                message.error(result.message || "连接失败");
            }
        } catch (error) {
            console.error("连接测试失败:", error);
            message.error(`连接测试失败: ${(error as Error).message || '网络或服务异常'}`);
        } finally {
            setTestingConnection(false);
        }
    };

    const handleGetTables = async () => {
        try {
            const values = await form.validateFields();
            setLoadingTables(true);
            
            // 后端服务地址，使用相对路径（部署时自动适应）
            const baseUrl = '';
            const tablesUrl = `${baseUrl}/api/tables`;
            
            // 构建请求体，包含数据源类型
            const requestBody = {
                dataSourceType,
                ...values
            };
            
            const response = await fetch(tablesUrl, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(requestBody),
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.code === 0 && result.data) {
                setTables(result.data);
                message.success(`成功获取 ${result.data.length} 个表`);
            } else {
                message.error(result.message || "获取表列表失败");
            }
        } catch (error) {
            console.error("获取表列表失败:", error);
            message.error(`获取表列表失败: ${(error as Error).message || '网络或服务异常'}`);
        } finally {
            setLoadingTables(false);
        }
    };

    return (
        <div className="app-container">
            <Card className="config-card">
                <Title level={4}>数据同步配置</Title>
                <Text type="secondary">
                    配置数据源连接信息，将数据同步到飞书多维表格
                </Text>

                <Divider />

                {/* 数据源类型选择 */}
                <Form.Item label="数据源类型">
                    <Radio.Group
                        value={dataSourceType}
                        onChange={handleDataSourceChange}
                    >
                        <Radio.Button value="maxcompute">
                            MaxCompute
                        </Radio.Button>
                        <Radio.Button value="sqlserver">
                            SQL Server
                        </Radio.Button>
                    </Radio.Group>
                </Form.Item>

                <Divider />

                <Form
                    form={form}
                    name="data-source-config"
                    layout="vertical"
                    onFinish={handleSaveConfig}
                    autoComplete="off"
                >
                    {dataSourceType === "maxcompute" ? (
                        <>
                            {/* MaxCompute 配置 */}
                            <Title level={5}>MaxCompute 连接信息</Title>

                            <Form.Item
                                label="AccessKey ID"
                                name="accessId"
                                rules={[
                                    {
                                        required: true,
                                        message: "请输入 AccessKey ID",
                                    },
                                ]}
                                tooltip="阿里云账号的 AccessKey ID"
                            >
                                <Input placeholder="请输入 AccessKey ID" />
                            </Form.Item>

                            <Form.Item
                                label="AccessKey Secret"
                                name="accessKey"
                                rules={[
                                    {
                                        required: true,
                                        message: "请输入 AccessKey Secret",
                                    },
                                ]}
                                tooltip="阿里云账号的 AccessKey Secret"
                            >
                                <Input.Password placeholder="请输入 AccessKey Secret" />
                            </Form.Item>

                            <Form.Item
                                label="服务端点 (Endpoint)"
                                name="endpoint"
                                rules={[
                                    {
                                        required: true,
                                        message: "请选择服务端点",
                                    },
                                ]}
                                tooltip="MaxCompute 服务所在的地域端点"
                                initialValue="http://service.cn.maxcompute.aliyun.com/api"
                            >
                                <Select
                                    options={ENDPOINT_OPTIONS}
                                    placeholder="请选择服务端点"
                                    showSearch
                                    optionFilterProp="label"
                                />
                            </Form.Item>

                            <Form.Item
                                label="项目名称 (Project)"
                                name="projectName"
                                rules={[
                                    {
                                        required: true,
                                        message: "请输入项目名称",
                                    },
                                ]}
                                tooltip="MaxCompute 项目名称"
                            >
                                <Input placeholder="请输入项目名称" />
                            </Form.Item>

                            <Form.Item
                                label="Schema 名称"
                                name="schemaName"
                                tooltip="Schema 名称，默认为 default"
                                initialValue="default"
                            >
                                <Input placeholder="default" />
                            </Form.Item>
                        </>
                    ) : (
                        <>
                            {/* SQL Server 配置 */}
                            <Title level={5}>SQL Server 连接信息</Title>

                            <Form.Item
                                label="服务器地址"
                                name="server"
                                rules={[
                                    {
                                        required: true,
                                        message: "请输入服务器地址",
                                    },
                                ]}
                                tooltip="SQL Server 服务器地址或 IP"
                            >
                                <Input placeholder="例如: localhost 或 192.168.1.100" />
                            </Form.Item>

                            <Form.Item
                                label="端口号"
                                name="port"
                                tooltip="SQL Server 端口号，默认 1433"
                                initialValue={1433}
                            >
                                <Input type="number" placeholder="1433" />
                            </Form.Item>

                            <Form.Item
                                label="数据库名称"
                                name="database"
                                rules={[
                                    {
                                        required: true,
                                        message: "请输入数据库名称",
                                    },
                                ]}
                                tooltip="要连接的数据库名称"
                            >
                                <Input placeholder="请输入数据库名称" />
                            </Form.Item>

                            <Form.Item
                                label="用户名"
                                name="user"
                                rules={[
                                    { required: true, message: "请输入用户名" },
                                ]}
                                tooltip="SQL Server 登录用户名"
                            >
                                <Input placeholder="请输入用户名" />
                            </Form.Item>

                            <Form.Item
                                label="密码"
                                name="password"
                                rules={[
                                    { required: true, message: "请输入密码" },
                                ]}
                                tooltip="SQL Server 登录密码"
                            >
                                <Input.Password placeholder="请输入密码" />
                            </Form.Item>

                            <Form.Item
                                label="Schema 名称"
                                name="schema"
                                tooltip="Schema 名称，默认为 dbo"
                                initialValue="dbo"
                            >
                                <Input placeholder="dbo" />
                            </Form.Item>

                            <Form.Item
                                label="启用加密连接"
                                name="encrypt"
                                valuePropName="checked"
                                initialValue={true}
                            >
                                <Switch />
                            </Form.Item>

                            <Form.Item
                                label="信任服务器证书"
                                name="trustServerCertificate"
                                valuePropName="checked"
                                initialValue={false}
                            >
                                <Switch />
                            </Form.Item>
                        </>
                    )}

                    <Divider />

                    {/* 通用数据源配置 */}
                    <Title level={5}>数据源信息</Title>

                    <Form.Item
                        label="表名"
                        name="tableName"
                        rules={[{ required: true, message: "请选择表名" }]}
                        tooltip="要同步的表名"
                    >
                        <Select
                            showSearch
                            placeholder="请搜索并选择表名"
                            loading={loadingTables}
                            filterOption={(input, option) =>
                                ((option?.label ?? option?.value) as string)
                                    .toLowerCase()
                                    .includes(input.toLowerCase())
                            }
                            onFocus={handleGetTables}
                        >
                            {tables.map((table) => (
                                <Select.Option key={table.name} value={table.name}>
                                    {table.name}
                                </Select.Option>
                            ))}
                        </Select>
                    </Form.Item>

                    <Form.Item
                        label="主键字段"
                        name="primaryField"
                        tooltip="表的主键字段名，用于唯一标识记录"
                    >
                        <Input placeholder="可选，默认为第一个字段或自动识别" />
                    </Form.Item>



                    <Divider />

                    {/* 高级选项 */}
                    <Title level={5}>高级选项</Title>

                    <Form.Item
                        label="自定义 SQL"
                        name="sql"
                        tooltip="自定义 SQL 查询语句，留空则同步整个表"
                    >
                        <TextArea
                            rows={3}
                            placeholder="可选，例如: SELECT * FROM your_table WHERE status = 'active'"
                        />
                    </Form.Item>

                    <Form.Item
                        label="批量处理记录数"
                        name="limit"
                        tooltip="每次批量处理的最大记录数，系统会自动处理所有数据"
                        initialValue={1000}
                    >
                        <Input type="number" placeholder="1000" />
                    </Form.Item>

                    <Divider />

                    {/* 操作按钮 */}
                    <Form.Item>
                        <div className="button-group">
                            <Button
                                onClick={handleTestConnection}
                                style={{ marginRight: 8 }}
                                loading={testingConnection}
                            >
                                测试连接
                            </Button>
                            <Button
                                type="primary"
                                htmlType="submit"
                                loading={loading}
                            >
                                保存并下一步
                            </Button>
                        </div>
                    </Form.Item>
                </Form>

                {userId && (
                    <div className="user-info">
                        <Text type="secondary" style={{ fontSize: 12 }}>
                            用户ID: {userId} | 租户Key: {tenantKey}
                        </Text>
                    </div>
                )}
            </Card>
        </div>
    );
}
