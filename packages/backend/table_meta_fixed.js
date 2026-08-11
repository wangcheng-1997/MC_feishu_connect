const { MaxComputeClient } = require('./maxcompute_client_fixed.js');
const { generateTableMeta } = require('./maxcompute_adapter.js');

async function getTableMetaFromMaxCompute(config) {
  try {
    const client = new MaxComputeClient(config);

    let columns = [];
    let tableName = config.tableName;

    if (config.sql) {
      const queryMeta = await client.getQueryMeta(config.sql);
      columns = queryMeta.Table.Columns || [];
      tableName = tableName || queryMeta.Table.Name || 'custom_query';
    } else if (config.tableName) {
      const tableMeta = await client.getTableMeta(config.tableName);
      columns = tableMeta.Table.Columns || [];
    } else {
      throw new Error('Missing tableName or sql in maxcompute config');
    }

    if (!columns || columns.length === 0) {
      throw new Error('Unable to infer columns from MaxCompute SQL/table metadata');
    }

    return generateTableMeta(tableName, columns, config.primaryField);
  } catch (error) {
    console.error('获取 MaxCompute 表元数据失败:', error);
    throw error;
  }
}

async function getTableMeta(reqBody = {}) {
  if (reqBody && reqBody.maxcompute) {
    return await getTableMetaFromMaxCompute(reqBody.maxcompute);
  }
  throw new Error('Missing maxcompute config');
}

module.exports = { getTableMeta, getTableMetaFromMaxCompute };
