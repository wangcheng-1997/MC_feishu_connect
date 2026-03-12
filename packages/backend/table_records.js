const { MaxComputeClient } = require('./maxcompute_client.js');
const { generateTableRecords, generateTableMeta } = require('./maxcompute_adapter.js');

/**
 * 从 MaxCompute 获取表记录数据
 * 
 * @param {Object} config - MaxCompute 连接配置
 * @param {string} config.accessId - 阿里云 AccessKey ID
 * @param {string} config.accessKey - 阿里云 AccessKey Secret
 * @param {string} config.endpoint - MaxCompute 服务端点，如: https://service.cn-hangzhou.maxcompute.aliyun.com/api
 * @param {string} config.region - 区域代码（可选），如: cn-hangzhou
 * @param {string} config.networkType - 网络类型（可选）: 'public' (公网), 'vpc' (VPC), 'intranet' (云产品互联)
 * @param {string} config.projectName - MaxCompute 项目名称
 * @param {string} config.tableName - 要同步的表名
 * @param {string} config.schemaName - Schema 名称（可选）
 * @param {string} config.sql - 自定义 SQL 查询（可选）
 * @param {number} config.limit - 每次获取记录数限制（默认 1000）
 * @param {number} config.offset - 偏移量（用于分页）
 * @param {Object} fields - 字段定义（用于数据转换）
 * @returns {Object} 飞书多维表格格式的记录数据
 */
async function getTableRecordsFromMaxCompute(config, fields) {
  try {
    const client = new MaxComputeClient(config);
    
    let data;
    
    // 如果提供了自定义 SQL，则执行自定义查询
    if (config.sql) {
      data = await client.executeSQL(config.sql);
    } else {
      // 否则查询整个表
      const limit = config.limit || 1000;
      const offset = config.offset || 0;
      data = await client.getTableData(config.tableName, limit, offset);
    }
    
    // 转换数据格式
    const hasMore = data.length >= (config.limit || 1000);
    const nextPageToken = hasMore ? String((config.offset || 0) + (config.limit || 1000)) : '';
    
    return generateTableRecords(data, fields, hasMore, nextPageToken);
  } catch (error) {
    console.error('获取 MaxCompute 表记录失败:', error);
    // 返回默认数据作为 fallback
    return getDefaultTableRecords();
  }
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
          fid_1: "1",
          fid_2: "示例记录1",
          fid_3: 100.50,
          fid_4: Date.now(),
          fid_5: true,
        },
      },
      {
        primaryId: "record_2",
        data: {
          fid_1: "2",
          fid_2: "示例记录2",
          fid_3: 200.75,
          fid_4: Date.now() - 86400000,
          fid_5: false,
        },
      },
      {
        primaryId: "record_3",
        data: {
          fid_1: "3",
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
 * 同步入口函数
 * 根据请求参数返回表记录数据
 * 
 * @param {Object} reqBody - 请求体，包含 MaxCompute 配置和字段定义
 * @returns {Object} 表记录数据
 */
async function getTableRecords(reqBody = {}) {
  // 如果请求中包含 MaxCompute 配置，则使用配置获取数据
  if (reqBody && reqBody.maxcompute) {
    // 如果没有提供字段定义，先获取表元数据
    let fields = reqBody.fields;
    if (!fields) {
      const { getTableMetaFromMaxCompute } = require('./table_meta.js');
      const meta = await getTableMetaFromMaxCompute(reqBody.maxcompute);
      fields = meta.fields;
    }
    
    return await getTableRecordsFromMaxCompute(reqBody.maxcompute, fields);
  }
  
  // 否则返回默认数据
  return getDefaultTableRecords();
}

module.exports = { 
  getTableRecords, 
  getTableRecordsFromMaxCompute, 
  getDefaultTableRecords 
};
