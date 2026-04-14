const { MaxComputeClient } = require('./maxcompute_client.js');
const { generateTableMeta } = require('./maxcompute_adapter.js');

function inferTableNameFromSql(sql) {
  if (!sql || typeof sql !== 'string') {
    return '';
  }
  const match = sql.match(/FROM\s+[`"]?([a-zA-Z0-9_.]+)[`"]?/i);
  return match && match[1] ? match[1] : '';
}

async function getTableMetaFromMaxCompute(config) {
  try {
    const client = new MaxComputeClient(config);

    let columns = [];
    let tableName = config.tableName;

    if (config.sql) {
      const inferredTableName = config.tableName || inferTableNameFromSql(config.sql);

      if (inferredTableName) {
        const tableMeta = await client.getTableMeta(inferredTableName);
        columns = tableMeta.Table.Columns || [];
        tableName = tableName || inferredTableName;
      } else {
        const data = await client.executeSQL(`SELECT * FROM (${config.sql}) t LIMIT 1`);
        if (Array.isArray(data) && data.length > 0) {
          const firstRow = data[0];
          columns = Object.keys(firstRow).map((key) => ({
            Name: key,
            Type: 'STRING',
            Comment: '',
          }));
        }
        tableName = tableName || 'SQL Query Result';
      }
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

