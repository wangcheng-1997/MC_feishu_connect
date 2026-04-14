const { MaxComputeClient } = require('./maxcompute_client.js');
const { generateTableRecords } = require('./maxcompute_adapter.js');
const { getSqlServerTableRecords } = require('./sqlserver_handler.js');

function ensureFields(fields, data) {
  if (Array.isArray(fields) && fields.length > 0) {
    return fields;
  }
  if (Array.isArray(data) && data.length > 0) {
    return Object.keys(data[0]).map((key, index) => ({
      fieldId: `fid_${index + 1}`,
      fieldName: key,
      fieldType: 1,
      isPrimary: index === 0,
      description: '',
    }));
  }
  return [];
}

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
 * @param {Object} fields - 字段定义（用于数据转换）
 * @param {number} offset - 偏移量（用于分批，默认0）
 * @param {number} limit - 每批记录数（默认1000，最大1000）
 * @returns {Object} 飞书多维表格格式的记录数据
 */
async function getTableRecordsFromMaxCompute(config, fields, offset = 0, limit = 1000) {
  try {
    const client = new MaxComputeClient(config);
    
    const batchSize = Math.min(limit, 1000);
    
    console.log(`[getTableRecordsFromMaxCompute] offset=${offset}, limit=${limit}, batchSize=${batchSize}`);
    
    let data;
    
    if (config.sql) {
      data = await client.executeSQL(config.sql, batchSize, offset);
      
      console.log(`[executeSQL返回] data.length=${data.length}, batchSize=${batchSize}, hasMore=${data.length === batchSize}`);
      
      const hasMore = data.length === batchSize;
      const nextPageToken = hasMore ? String(offset + batchSize) : '';
      
      return generateTableRecords(data, ensureFields(fields, data), hasMore, nextPageToken, offset);
    } else {
      data = await client.getTableData(config.tableName, batchSize, offset);
      
      console.log(`[getTableData返回] data.length=${data.length}, batchSize=${batchSize}, hasMore=${data.length === batchSize}`);
      
      const hasMore = data.length === batchSize;
      const nextPageToken = hasMore ? String(offset + batchSize) : '';
      
      return generateTableRecords(data, ensureFields(fields, data), hasMore, nextPageToken, offset);
    }
  } catch (error) {
    console.error('获取 MaxCompute 表记录失败:', error);
    throw error;
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
 * @param {number} reqBody.offset - 偏移量（用于分批，默认0）
 * @param {number} reqBody.limit - 每批记录数（默认1000，最大1000）
 * @returns {Object} 表记录数据
 */
async function getTableRecords(reqBody = {}) {
  // 如果请求中包含 MaxCompute 配置，则使用配置获取数据
  if (reqBody && reqBody.maxcompute) {
    // 如果没有提供字段定义，先获取表元数据
    let fields = reqBody.fields;
    if (!fields || fields.length === 0) {
      try {
        const { getTableMetaFromMaxCompute } = require('./table_meta_fixed.js');
        const meta = await getTableMetaFromMaxCompute(reqBody.maxcompute);
        fields = meta.fields;
      } catch (metaError) {
        console.warn('[getTableRecords] MaxCompute metadata failed, fallback to infer fields from row:', metaError.message);
      }
    }

    // 获取分页参数
    const offset = parseInt(reqBody.offset) || 0;
    const limit = Math.min(parseInt(reqBody.limit) || 1000, 1000);
    
    return await getTableRecordsFromMaxCompute(reqBody.maxcompute, fields, offset, limit);
  }
  
  // 如果请求中包含 SQL Server 配置
  if (reqBody && reqBody.sqlserver) {
    // 如果没有提供字段定义，先获取表元数据
    let fields = reqBody.fields;
    if (!fields) {
      const { getSqlServerTableMeta } = require('./sqlserver_handler.js');
      const meta = await getSqlServerTableMeta(reqBody.sqlserver);
      fields = meta.fields;
    }
    
    // 获取分页参数
    const offset = parseInt(reqBody.offset) || 0;
    const limit = Math.min(parseInt(reqBody.limit) || 1000, 1000);
    
    return await getSqlServerTableRecords(reqBody.sqlserver, fields, offset, limit);
  }
  
  // 否则返回默认数据
  throw new Error('Missing data source config: maxcompute or sqlserver');
}

module.exports = { 
  getTableRecords, 
  getTableRecordsFromMaxCompute, 
  getDefaultTableRecords 
};
