const { MaxComputeClient } = require('./maxcompute_client_fixed.js');
const { generateTableMeta, generateTableRecords } = require('./maxcompute_adapter.js');
const { getSqlServerTableRecords } = require('./sqlserver_handler.js');
const {
  createSyncStageLogger,
  isDetailedSyncLogging,
  logSyncEvent,
  logSyncFailure,
} = require('./sync_logger.js');
const {
  CACHE_DIR,
  buildPageToken,
  parsePageToken,
  buildQuerySignature,
  getTaskPage,
  cleanupExpiredTasks,
  getTaskFileSize,
} = require('./maxcompute_file_cache.js');

function hasFields(fields) {
  if (Array.isArray(fields) && fields.length > 0) {
    return true;
  }
  return false;
}

function buildMaxComputeSyncContext(config, traceId, tokenInfo = null) {
  const object = config.sql ? 'query' : 'table';
  const fallbackTableId = config.tableId || tokenInfo?.tableId || 'unknown';
  if (config.syncContext) {
    return {
      ...config.syncContext,
      tableId: config.syncContext.tableId && config.syncContext.tableId !== 'unknown'
        ? config.syncContext.tableId
        : fallbackTableId,
    };
  }
  return {
    traceId,
    tableId: fallbackTableId,
    source: 'maxcompute',
    object,
    sourceName: object === 'query' ? (config.tableName || 'custom_query') : (config.tableName || 'unknown'),
  };
}

function buildFieldsFromCachedResult(config, cachedResult) {
  const columns = Array.isArray(cachedResult.columns) ? cachedResult.columns : [];
  if (columns.length === 0) {
    throw new Error('Missing field definitions for record conversion');
  }
  return generateTableMeta(config.tableName || 'custom_query', columns, config.primaryField).fields;
}

function buildFieldsFromColumns(config, columns) {
  if (!Array.isArray(columns) || columns.length === 0) {
    return null;
  }
  return generateTableMeta(config.tableName || 'custom_query', columns, config.primaryField).fields;
}

function buildPageStage(pageOffset, pageSize, totalRows) {
  const pageIndex = Math.floor((parseInt(pageOffset, 10) || 0) / pageSize) + 1;
  const totalPages = totalRows > 0 ? Math.max(Math.ceil(totalRows / pageSize), 1) : '?';
  return `${pageIndex}/${totalPages}`;
}

function buildSourceSQL(config) {
  if (config.sql) {
    return config.sql;
  }
  return `SELECT * FROM ${config.tableName}`;
}

async function executeSourceToPageCache(client, config, querySignature, batchSize) {
  return await client.executeSQLToCache(
    buildSourceSQL(config),
    CACHE_DIR,
    querySignature,
    batchSize
  );
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
  const traceId = config.traceId || `mc_${Date.now().toString(36)}`;
  const startAt = Date.now();
  const batchSize = Math.min(limit, 1000);
  const tokenInfo = parsePageToken(pageToken);
  const syncContext = buildMaxComputeSyncContext(config, traceId, tokenInfo);
  const detailedLogging = isDetailedSyncLogging();
  const logStage = detailedLogging ? createSyncStageLogger(syncContext, tokenInfo ? 4 : 3) : null;

  try {
    const client = new MaxComputeClient(config);
    let normalizedFields = hasFields(fields) ? fields : null;
    const querySignature = buildQuerySignature(config);

    if (detailedLogging) {
      logStage(1, 'request', {
        offset,
        limit,
        pageSize: batchSize,
        hasPageToken: Boolean(pageToken),
      });
    } else if (!tokenInfo) {
      logSyncEvent(syncContext, {
        stage: 'start',
        stageName: 'source_fetch_start',
        stageMs: 0,
        elapsedMs: 0,
        offset,
        limit,
        pageSize: batchSize,
      });
    }
    await cleanupExpiredTasks();

    if (tokenInfo) {
      const pageOffset = tokenInfo.offset;
      let taskId = tokenInfo.taskId;
      let pageRows = null;
      let totalRows = 0;
      let hasMore = false;
      let cachePayload = null;
      let rebuiltCache = false;
      let expiresInMs = '';
      let rebuildTiming = null;
      let cacheFileBytes = '';

      try {
        const { payload, page } = await getTaskPage(tokenInfo.taskId, pageOffset, querySignature, { pageSize: batchSize });
        cachePayload = payload;
        if (payload.pageSize && payload.pageSize !== batchSize) {
          throw new Error('Cache page size mismatch');
        }
        if (!page || !Array.isArray(page.rows)) {
          throw new Error('Cache page missing');
        }
        pageRows = page.rows;
        totalRows = payload.total || 0;
        hasMore = Boolean(page.hasMore);
        expiresInMs = payload.expiresAt ? Math.max(payload.expiresAt - Date.now(), 0) : '';
        if (detailedLogging) {
          logStage(2, 'cache_hit', {
            taskId,
            pageOffset,
            pageSize: pageRows.length,
            total: totalRows,
            expiresInMs,
          });
        }
      } catch (cacheError) {
        rebuiltCache = true;
        if (detailedLogging) {
          logStage(2, 'cache_miss', { taskId, pageOffset, reason: cacheError.message });
        }
        const cachedResult = await executeSourceToPageCache(client, config, querySignature, batchSize);
        taskId = cachedResult.taskId;
        totalRows = cachedResult.total || 0;
        rebuildTiming = cachedResult.timing || null;
        const { payload, page } = await getTaskPage(taskId, pageOffset, querySignature, { pageSize: batchSize });
        cachePayload = payload;
        pageRows = page && Array.isArray(page.rows) ? page.rows : [];
        hasMore = page ? Boolean(page.hasMore) : false;
        cacheFileBytes = await getTaskFileSize(taskId).catch(() => 0);
        if (detailedLogging) {
          logStage(3, 'source_rebuild', {
            taskId,
            pageOffset,
            total: totalRows,
            pageSize: pageRows.length,
            executeMs: cachedResult.timing?.executeMs,
            waitMs: cachedResult.timing?.waitMs,
            readWriteMs: cachedResult.timing?.readWriteMs,
            cacheFileBytes,
          });
        }
      }

      if (pageRows) {
        let fieldSource = normalizedFields ? 'request_fields' : '';
        if (!normalizedFields) {
          normalizedFields = buildFieldsFromColumns(config, cachePayload?.columns);
          fieldSource = normalizedFields ? 'cache_columns' : '';
        }
        if (!normalizedFields) {
          const meta = await require('./table_meta_fixed.js').getTableMetaFromMaxCompute(config);
          normalizedFields = meta.fields;
          fieldSource = 'meta_probe';
        }
        const nextPageToken = hasMore ? buildPageToken(taskId, pageOffset + batchSize, syncContext.tableId) : '';
        const doneDetails = {
          taskId,
          pageOffset,
          pageSize: pageRows.length,
          hasMore,
          nextOffset: hasMore ? pageOffset + batchSize : '',
          totalDurationMs: Date.now() - startAt,
        };
        if (detailedLogging && !rebuiltCache) {
          logStage(3, 'build_records', { pageSize: pageRows.length, fieldSource });
        }
        if (detailedLogging) {
          logStage(4, 'done', doneDetails);
        } else {
          const elapsedMs = Date.now() - startAt;
          logSyncEvent(syncContext, {
            stage: buildPageStage(pageOffset, batchSize, totalRows),
            stageName: rebuiltCache ? 'source_rebuild' : 'cache_hit',
            stageMs: elapsedMs,
            elapsedMs,
            taskId,
            pageOffset,
            pageSize: pageRows.length,
            total: totalRows,
            hasMore,
            nextOffset: hasMore ? pageOffset + batchSize : '',
            fieldSource,
            expiresInMs,
            executeMs: rebuildTiming?.executeMs,
            waitMs: rebuildTiming?.waitMs,
            readWriteMs: rebuildTiming?.readWriteMs,
            cacheFileBytes,
          });
        }
        return generateTableRecords(pageRows, normalizedFields, hasMore, nextPageToken, pageOffset);
      }
      throw new Error('Invalid cache task data');
    }

    const pageOffset = Math.max(parseInt(offset, 10) || 0, 0);
    const cachedResult = await executeSourceToPageCache(client, config, querySignature, batchSize);
    const taskId = cachedResult.taskId;
    if (!normalizedFields) {
      normalizedFields = buildFieldsFromCachedResult(config, cachedResult);
    }
    const pageRows = Array.isArray(cachedResult.data) ? cachedResult.data : [];
    const hasMore = Boolean(cachedResult.hasMore);
    const cacheFileBytes = await getTaskFileSize(taskId).catch(() => 0);
    const sourceFetchDetails = {
      taskId,
      pageOffset,
      total: cachedResult.total || 0,
      pageSize: pageRows.length,
      hasMore,
      executeMs: cachedResult.timing?.executeMs,
      waitMs: cachedResult.timing?.waitMs,
      readWriteMs: cachedResult.timing?.readWriteMs,
      cacheFileBytes,
    };
    if (detailedLogging) {
      logStage(2, 'source_fetch', sourceFetchDetails);
    }
    const nextPageToken = hasMore ? buildPageToken(taskId, pageOffset + batchSize, syncContext.tableId) : '';
    const doneDetails = {
      taskId,
      pageOffset,
      pageSize: pageRows.length,
      hasMore,
      nextOffset: hasMore ? pageOffset + batchSize : '',
      totalDurationMs: Date.now() - startAt,
    };
    if (detailedLogging) {
      logStage(3, 'done', doneDetails);
    } else {
      const elapsedMs = Date.now() - startAt;
      logSyncEvent(syncContext, {
        stage: buildPageStage(pageOffset, batchSize, cachedResult.total || pageRows.length),
        stageName: 'source_fetch',
        stageMs: elapsedMs,
        elapsedMs,
        nextOffset: hasMore ? pageOffset + batchSize : '',
        fieldSource: hasFields(fields) ? 'request_fields' : 'cache_columns',
        ...sourceFetchDetails,
      });
    }
    return generateTableRecords(pageRows, normalizedFields, hasMore, nextPageToken, pageOffset);
  } catch (error) {
    logSyncFailure(syncContext, {
      totalDurationMs: Date.now() - startAt,
      message: error.message,
    });
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
    // 获取分页参数
    const offset = parseInt(reqBody.offset) || 0;
    const limit = Math.min(parseInt(reqBody.limit) || 1000, 1000);
    const pageToken = reqBody.pageToken || reqBody.nextPageToken || '';
    const maxcomputeConfig = {
      ...reqBody.maxcompute,
      traceId: reqBody.maxcompute.traceId || reqBody.traceId,
    };
    
    return await getTableRecordsFromMaxCompute(maxcomputeConfig, reqBody.fields, offset, limit, pageToken);
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
