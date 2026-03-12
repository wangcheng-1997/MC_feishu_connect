const { MaxComputeClient } = require('./maxcompute_client.js');
const { generateTableMeta } = require('./maxcompute_adapter.js');

/**
 * 获取表元数据
 * 支持从 MaxCompute 动态获取表结构
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
 * @param {string} config.primaryField - 主键字段名（可选）
 * @returns {Object} 飞书多维表格格式的表元数据
 */
async function getTableMetaFromMaxCompute(config) {
  try {
    const client = new MaxComputeClient(config);
    
    // 获取 MaxCompute 表元数据
    const tableMeta = await client.getTableMeta(config.tableName);
    
    // 提取列信息
    const columns = tableMeta.Table.Columns || [];
    
    // 转换为飞书多维表格格式
    return generateTableMeta(
      config.tableName,
      columns,
      config.primaryField
    );
  } catch (error) {
    console.error('获取 MaxCompute 表元数据失败:', error);
    // 返回默认元数据作为 fallback
    return getDefaultTableMeta();
  }
}

/**
 * 获取默认表元数据（示例/测试用）
 */
function getDefaultTableMeta() {
  return {
    tableName: "MaxCompute 数据表",
    fields: [
      {
        fieldId: "fid_1",
        fieldName: "ID",
        fieldType: 1,
        isPrimary: true,
        description: "主键ID",
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
          formatter: "yyyy-MM-dd HH:mm:ss",
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
 * 同步入口函数
 * 根据请求参数返回表元数据
 * 
 * @param {Object} reqBody - 请求体，包含 MaxCompute 配置
 * @returns {Object} 表元数据
 */
async function getTableMeta(reqBody = {}) {
  // 如果请求中包含 MaxCompute 配置，则使用配置获取元数据
  if (reqBody && reqBody.maxcompute) {
    return await getTableMetaFromMaxCompute(reqBody.maxcompute);
  }
  
  // 否则返回默认元数据
  return getDefaultTableMeta();
}

module.exports = { getTableMeta, getTableMetaFromMaxCompute, getDefaultTableMeta };
