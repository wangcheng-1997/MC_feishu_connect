const { SqlServerClient } = require('./sqlserver_client.js');
const { generateTableMeta, generateTableRecords } = require('./sqlserver_adapter.js');

/**
 * 获取默认表元数据（示例/测试用）
 */
function getDefaultTableMeta() {
  return {
    tableName: "SQL Server 数据表",
    fields: [
      {
        fieldId: "fid_1",
        fieldName: "ID",
        fieldType: 2,
        isPrimary: true,
        description: "主键ID",
        property: {
          formatter: "#,##0",
        },
      },
      {
        fieldId: "fid_2",
        fieldName: "名称",
        fieldType: 1,
        isPrimary: false,
        description: "名称",
      },
      {
        fieldId: "fid_3",
        fieldName: "数值",
        fieldType: 2,
        isPrimary: false,
        description: "数值字段",
        property: {
          formatter: "#,##0.00",
        },
      },
      {
        fieldId: "fid_4",
        fieldName: "创建时间",
        fieldType: 5,
        isPrimary: false,
        description: "创建时间",
        property: {
          formatter: "yyyy/MM/dd HH:mm:ss",
        },
      },
      {
        fieldId: "fid_5",
        fieldName: "状态",
        fieldType: 7,
        isPrimary: false,
        description: "状态标识",
      },
    ],
  };
}

/**
 * 获取默认表记录（示例/测试用）
 */
function getDefaultTableRecords() {
  return {
    nextPageToken: "",
    hasMore: false,
    records: [
      {
        primaryId: "record_1",
        data: {
          fid_1: 1,
          fid_2: "示例记录1",
          fid_3: 100.50,
          fid_4: Date.now(),
          fid_5: true,
        },
      },
      {
        primaryId: "record_2",
        data: {
          fid_1: 2,
          fid_2: "示例记录2",
          fid_3: 200.75,
          fid_4: Date.now() - 86400000,
          fid_5: false,
        },
      },
      {
        primaryId: "record_3",
        data: {
          fid_1: 3,
          fid_2: "示例记录3",
          fid_3: 300.00,
          fid_4: Date.now() - 172800000,
          fid_5: true,
        },
      },
    ],
  };
}

/**
 * 获取 SQL Server 表元数据
 * 
 * @param {Object} config - SQL Server 连接配置
 * @param {string} config.server - SQL Server 服务器地址
 * @param {number} config.port - 端口号（默认 1433）
 * @param {string} config.database - 数据库名称
 * @param {string} config.user - 用户名
 * @param {string} config.password - 密码
 * @param {string} config.tableName - 要同步的表名
 * @param {string} config.schema - Schema 名称（默认 dbo）
 * @param {string} config.primaryField - 主键字段名（可选）
 * @returns {Object} 飞书多维表格格式的表元数据
 */
async function getSqlServerTableMeta(config) {
  const client = new SqlServerClient(config);
  
  try {
    // 获取 SQL Server 表元数据
    const tableMeta = await client.getTableMeta(
      config.tableName, 
      config.schema || 'dbo'
    );
    
    // 转换为飞书多维表格格式
    return generateTableMeta(
      config.tableName,
      tableMeta.columns,
      config.primaryField
    );
  } catch (error) {
    console.error('获取 SQL Server 表元数据失败:', error);
    // 返回默认元数据作为 fallback
    return getDefaultTableMeta();
  } finally {
    await client.close();
  }
}

/**
 * 获取 SQL Server 表记录数据
 * 
 * @param {Object} config - SQL Server 连接配置
 * @param {Object} fields - 字段定义（用于数据转换）
 * @returns {Object} 飞书多维表格格式的记录数据
 */
async function getSqlServerTableRecords(config, fields) {
  const client = new SqlServerClient(config);
  
  try {
    let data;
    const schema = config.schema || 'dbo';
    
    // 如果提供了自定义 SQL，则执行自定义查询
    if (config.sql) {
      data = await client.executeQuery(config.sql);
    } else {
      // 否则查询整个表
      const limit = config.limit || 1000;
      const offset = config.offset || 0;
      data = await client.getTableData(config.tableName, schema, limit, offset);
    }
    
    // 如果没有提供字段定义，先获取表元数据
    if (!fields) {
      const meta = await getSqlServerTableMeta(config);
      fields = meta.fields;
    }
    
    // 转换数据格式
    const hasMore = data.length >= (config.limit || 1000);
    const nextPageToken = hasMore ? String((config.offset || 0) + (config.limit || 1000)) : '';
    
    return generateTableRecords(data, fields, hasMore, nextPageToken);
  } catch (error) {
    console.error('获取 SQL Server 表记录失败:', error);
    // 返回默认数据作为 fallback
    return getDefaultTableRecords();
  } finally {
    await client.close();
  }
}

module.exports = {
  getSqlServerTableMeta,
  getSqlServerTableRecords,
  getDefaultTableMeta,
  getDefaultTableRecords,
};
