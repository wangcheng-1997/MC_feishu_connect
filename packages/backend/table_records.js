const { MaxComputeClient } = require('./maxcompute_client_fixed.js');
const { generateTableRecords } = require('./maxcompute_adapter.js');
const { getSqlServerTableRecords } = require('./sqlserver_handler.js');
const {
  buildPageToken,
  parsePageToken,
  buildQuerySignature,
  createTask,
  getTaskPage,
  cleanupExpiredTasks,
  getTaskFileSize,
} = require('./maxcompute_file_cache.js');

function logTaskStage(stage, details = {}) {
  const parts = Object.entries(details)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${key}=${value}`);
  console.log(`[maxcompute_sync] stage=${stage}${parts.length ? ` ${parts.join(' ')}` : ''}`);
}

function ensureFields(fields) {
  if (Array.isArray(fields) && fields.length > 0) {
    return fields;
  }
  throw new Error('Missing field definitions for record conversion');
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
async function getTableRecordsFromMaxCompute(config, fields, offset = 0, limit = 1000, pageToken = '') {
  try {
    const traceId = config.traceId || `mc_${Date.now().toString(36)}`;
    const startAt = Date.now();
    const withTrace = (details = {}) => ({ traceId, ...details });
    const client = new MaxComputeClient(config);
    const normalizedFields = ensureFields(fields);
    const querySignature = buildQuerySignature(config);
    
    const batchSize = Math.min(limit, 1000);
    
    console.log(`[getTableRecordsFromMaxCompute] offset=${offset}, limit=${limit}, batchSize=${batchSize}`);
    logTaskStage('request_received', withTrace({ offset, limit, batchSize, hasPageToken: Boolean(pageToken), tokenPreview: pageToken ? String(pageToken).slice(0, 18) : '' }));
    await cleanupExpiredTasks();
    logTaskStage('cleanup_done', withTrace());

    const tokenInfo = parsePageToken(pageToken);
    if (tokenInfo) {
      const pageOffset = tokenInfo.offset;
      let taskId = tokenInfo.taskId;
      let pageRows = null;
      let totalRows = 0;
      let hasMore = false;
      logTaskStage('token_parsed', withTrace({ taskId, pageOffset }));
      try {
        const { payload, page } = await getTaskPage(tokenInfo.taskId, pageOffset, querySignature, { pageSize: batchSize });
        if (payload.pageSize && payload.pageSize !== batchSize) {
          throw new Error('Cache page size mismatch');
        }
        if (!page || !Array.isArray(page.rows)) {
          throw new Error('Cache page missing');
        }
        pageRows = page.rows;
        totalRows = payload.total || 0;
        hasMore = Boolean(page.hasMore);
        const expiresInMs = payload.expiresAt ? Math.max(payload.expiresAt - Date.now(), 0) : '';
        logTaskStage('cache_lookup_done', withTrace({ taskId, pageOffset, cacheHit: true, pageSize: pageRows.length, total: totalRows, expiresInMs }));
      } catch (cacheError) {
        console.warn(`[cacheMiss] ${cacheError.message}, recreate task for offset=${pageOffset}`);
        let allRows;
        const fetchStart = Date.now();
        logTaskStage('source_fetch_start', withTrace({ taskId: 'recreate', pageOffset: 0, batchSize: 'all', mode: config.sql ? 'sql' : 'table' }));
        if (config.sql) {
          allRows = await client.executeSQL(config.sql, null, 0);
        } else {
          allRows = await client.getTableData(config.tableName, null, 0);
        }
        logTaskStage('source_fetch_done', withTrace({ taskId: 'recreate', pageOffset: 0, fetched: Array.isArray(allRows) ? allRows.length : 0, durationMs: Date.now() - fetchStart }));
        const recreatedTask = await createTask(querySignature, allRows, { pageSize: batchSize });
        taskId = recreatedTask.taskId;
        totalRows = Array.isArray(allRows) ? allRows.length : 0;
        pageRows = Array.isArray(allRows) ? allRows.slice(pageOffset, pageOffset + batchSize) : [];
        hasMore = pageOffset + batchSize < totalRows;
        const cacheFileBytes = await getTaskFileSize(taskId).catch(() => 0);
        logTaskStage('task_recreated', withTrace({ oldTaskId: tokenInfo.taskId, taskId, pageOffset, reason: cacheError.message, total: totalRows, cacheFileBytes }));
      }

      if (pageRows) {
        const nextPageToken = hasMore ? buildPageToken(taskId, pageOffset + batchSize) : '';
        logTaskStage('return_cached_page', withTrace({
          taskId,
          pageOffset,
          pageSize: pageRows.length,
          hasMore,
          nextOffset: hasMore ? pageOffset + batchSize : '',
          durationMs: Date.now() - startAt,
        }));
        return generateTableRecords(pageRows, normalizedFields, hasMore, nextPageToken, pageOffset);
      }
      throw new Error('Invalid cache task data');
    }

    const pageOffset = Math.max(parseInt(offset, 10) || 0, 0);
    let allRows;
    const fetchStart = Date.now();
    logTaskStage('source_fetch_start', withTrace({ taskId: 'new', pageOffset: 0, batchSize: 'all', mode: config.sql ? 'sql' : 'table' }));
    if (config.sql) {
      allRows = await client.executeSQL(config.sql, null, 0);
    } else {
      allRows = await client.getTableData(config.tableName, null, 0);
    }
    logTaskStage('source_fetch_done', withTrace({ taskId: 'new', pageOffset: 0, fetched: Array.isArray(allRows) ? allRows.length : 0, durationMs: Date.now() - fetchStart }));
    const task = await createTask(querySignature, allRows, { pageSize: batchSize });
    const rows = Array.isArray(allRows) ? allRows : [];
    const pageRows = rows.slice(pageOffset, pageOffset + batchSize);
    const hasMore = pageOffset + batchSize < rows.length;
    const cacheFileBytes = await getTaskFileSize(task.taskId).catch(() => 0);
    logTaskStage('task_created', withTrace({ taskId: task.taskId, pageOffset, total: rows.length, cacheFileBytes }));
    logTaskStage('cache_write_done', withTrace({ taskId: task.taskId, pageOffset, hasMore, cacheFileBytes }));
    const nextPageToken = hasMore ? buildPageToken(task.taskId, pageOffset + batchSize) : '';

    console.log(`[cacheTask] taskId=${task.taskId}, total=${rows.length}, pageOffset=${pageOffset}, pageSize=${Array.isArray(pageRows) ? pageRows.length : 0}, hasMore=${hasMore}`);
    logTaskStage('return_fetched_page', withTrace({
      taskId: task.taskId,
      pageOffset,
      pageSize: Array.isArray(pageRows) ? pageRows.length : 0,
      hasMore,
      nextOffset: hasMore ? pageOffset + batchSize : '',
      durationMs: Date.now() - startAt,
    }));
    return generateTableRecords(pageRows, normalizedFields, hasMore, nextPageToken, pageOffset);
  } catch (error) {
    logTaskStage('failed', { message: error.message, traceId: config.traceId || '' });
    console.error('获取 MaxCompute 表记录失败:', error.message);
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
      const { getTableMetaFromMaxCompute } = require('./table_meta_fixed.js');
      const meta = await getTableMetaFromMaxCompute(reqBody.maxcompute);
      fields = meta.fields;
    }

    // 获取分页参数
    const offset = parseInt(reqBody.offset) || 0;
    const limit = Math.min(parseInt(reqBody.limit) || 1000, 1000);
    const pageToken = reqBody.pageToken || reqBody.nextPageToken || '';
    
    return await getTableRecordsFromMaxCompute(reqBody.maxcompute, fields, offset, limit, pageToken);
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
