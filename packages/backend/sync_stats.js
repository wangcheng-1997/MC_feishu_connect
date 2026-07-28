const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const DEFAULT_TIME_ZONE = 'Asia/Shanghai';
const DEFAULT_DAYS = 7;
const MAX_DAYS = 90;
const MAX_PREVIEW_LENGTH = 180;

let writeQueue = Promise.resolve();

function getStatsDir() {
  return process.env.SYNC_STATS_DIR
    ? path.resolve(process.env.SYNC_STATS_DIR)
    : path.join(__dirname, '.sync_stats');
}

function getTimeZone() {
  return process.env.SYNC_STATS_TZ || DEFAULT_TIME_ZONE;
}

function getDateKey(date = new Date(), timeZone = getTimeZone()) {
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(date);
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day}`;
  } catch (_) {
    return date.toISOString().slice(0, 10);
  }
}

function parseDateKey(value) {
  const text = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : '';
}

function addDays(dateKey, days) {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function buildDateRange(endDate, days) {
  const safeDays = Math.min(Math.max(parseInt(days, 10) || DEFAULT_DAYS, 1), MAX_DAYS);
  const end = parseDateKey(endDate) || getDateKey();
  const dates = [];
  for (let index = safeDays - 1; index >= 0; index -= 1) {
    dates.push(addDays(end, -index));
  }
  return dates;
}

async function ensureStatsDir() {
  const dir = getStatsDir();
  if (!fs.existsSync(dir)) {
    await fsp.mkdir(dir, { recursive: true });
  }
  return dir;
}

function buildStatsPath(dateKey) {
  return path.join(getStatsDir(), `${dateKey}.json`);
}

function createEmptyDailyStats(dateKey) {
  const now = new Date().toISOString();
  return {
    date: dateKey,
    timeZone: getTimeZone(),
    createdAt: now,
    updatedAt: now,
    totals: createEmptyCounts(),
    items: {},
  };
}

async function readDailyStats(dateKey) {
  const filePath = buildStatsPath(dateKey);
  try {
    const content = await fsp.readFile(filePath, 'utf8');
    const stats = JSON.parse(content);
    return stats && stats.date === dateKey ? stats : createEmptyDailyStats(dateKey);
  } catch (error) {
    if (error && error.code !== 'ENOENT') {
      throw error;
    }
    return createEmptyDailyStats(dateKey);
  }
}

async function writeDailyStats(dateKey, stats) {
  await ensureStatsDir();
  const filePath = buildStatsPath(dateKey);
  const tempPath = `${filePath}.${process.pid}.tmp`;
  await fsp.writeFile(tempPath, JSON.stringify(stats, null, 2), 'utf8');
  await fsp.rename(tempPath, filePath);
}

function enqueueWrite(work) {
  const next = writeQueue.catch(() => {}).then(work);
  writeQueue = next.catch(() => {});
  return next;
}

function createEmptyCounts() {
  return {
    totalCalls: 0,
    successCalls: 0,
    failedCalls: 0,
    tableMetaCalls: 0,
    recordCalls: 0,
    recordFirstPageCalls: 0,
    recordPageTokenCalls: 0,
    rowsReturned: 0,
    totalDurationMs: 0,
    maxDurationMs: 0,
  };
}

function addCount(counts, key, amount = 1) {
  counts[key] = (Number(counts[key]) || 0) + amount;
}

function updateCounts(counts, event) {
  addCount(counts, 'totalCalls');
  addCount(counts, event.status === 'failed' ? 'failedCalls' : 'successCalls');
  if (event.endpoint === 'table_meta') {
    addCount(counts, 'tableMetaCalls');
  }
  if (event.endpoint === 'records') {
    addCount(counts, 'recordCalls');
    addCount(counts, event.hasPageToken ? 'recordPageTokenCalls' : 'recordFirstPageCalls');
  }
  addCount(counts, 'rowsReturned', Math.max(parseInt(event.pageSize, 10) || 0, 0));
  const durationMs = Math.max(parseInt(event.durationMs, 10) || 0, 0);
  addCount(counts, 'totalDurationMs', durationMs);
  counts.maxDurationMs = Math.max(Number(counts.maxDurationMs) || 0, durationMs);
}

function normalizeText(value, maxLength = 300) {
  if (value === undefined || value === null) return '';
  return String(value).replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function hashText(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 12);
}

function normalizeSql(sql) {
  return normalizeText(sql, 4000);
}

function getSourceInfo(config = {}, context = {}) {
  const sourceConfig = config.maxcompute || config.sqlserver || {};
  const source = context.source || (config.maxcompute ? 'maxcompute' : config.sqlserver ? 'sqlserver' : 'unknown');
  const objectType = context.object || (sourceConfig.sql ? 'query' : 'table');
  const sourceTableName = normalizeText(sourceConfig.tableName || '');
  const sql = normalizeSql(sourceConfig.sql || '');
  const queryHash = sql ? hashText(sql) : '';
  const objectName = normalizeText(
    context.sourceName || (objectType === 'query' ? sourceTableName || 'custom_query' : sourceTableName || 'unknown')
  );

  return {
    source,
    objectType,
    objectName,
    sourceTableName,
    queryHash,
    queryPreview: sql ? sql.slice(0, MAX_PREVIEW_LENGTH) : '',
    sourceKey: objectType === 'query'
      ? queryHash || objectName || 'custom_query'
      : sourceTableName || objectName || 'unknown',
  };
}

function getFeishuInfo(config = {}, context = {}) {
  return {
    tableId: normalizeText(context.tableId || config.tableId || 'unknown', 120) || 'unknown',
    tableName: normalizeText(config.feishuTableName || config.larkTableName || config.bitableTableName || '', 120),
  };
}

function buildItemKey(feishuInfo, sourceInfo) {
  return [
    feishuInfo.tableId || 'unknown',
    sourceInfo.source || 'unknown',
    sourceInfo.objectType || 'unknown',
    sourceInfo.sourceKey || 'unknown',
  ].join('::');
}

function createItem(key, feishuInfo, sourceInfo, event) {
  return {
    key,
    feishuTableId: feishuInfo.tableId,
    feishuTableName: feishuInfo.tableName,
    source: sourceInfo.source,
    objectType: sourceInfo.objectType,
    objectName: sourceInfo.objectName,
    sourceTableName: sourceInfo.sourceTableName,
    queryHash: sourceInfo.queryHash,
    queryPreview: sourceInfo.queryPreview,
    firstSeenAt: event.calledAt,
    lastSeenAt: event.calledAt,
    lastTraceId: event.traceId || '',
    lastError: '',
    lastRequestSource: {},
    counts: createEmptyCounts(),
  };
}

async function recordSyncStat(input = {}) {
  const calledAt = new Date();
  const dateKey = getDateKey(calledAt);
  const event = {
    ...input,
    calledAt: calledAt.toISOString(),
    status: input.status === 'failed' ? 'failed' : 'success',
  };

  return enqueueWrite(async () => {
    const stats = await readDailyStats(dateKey);
    const sourceInfo = getSourceInfo(event.config, event.context);
    const feishuInfo = getFeishuInfo(event.config, event.context);
    const key = buildItemKey(feishuInfo, sourceInfo);
    const item = stats.items[key] || createItem(key, feishuInfo, sourceInfo, event);

    item.feishuTableId = feishuInfo.tableId;
    item.feishuTableName = feishuInfo.tableName || item.feishuTableName || '';
    item.objectName = sourceInfo.objectName || item.objectName;
    item.sourceTableName = sourceInfo.sourceTableName || item.sourceTableName;
    item.queryPreview = sourceInfo.queryPreview || item.queryPreview;
    item.lastSeenAt = event.calledAt;
    item.lastTraceId = event.traceId || event.context?.traceId || item.lastTraceId || '';
    item.lastError = event.status === 'failed' ? normalizeText(event.errorMessage || '', 300) : item.lastError || '';
    item.lastRequestSource = event.requestSource || item.lastRequestSource || {};

    updateCounts(item.counts, event);
    updateCounts(stats.totals, event);

    stats.items[key] = item;
    stats.updatedAt = event.calledAt;
    await writeDailyStats(dateKey, stats);
  });
}

function mergeCounts(target, source) {
  const merged = { ...target };
  for (const [key, value] of Object.entries(source || {})) {
    if (key === 'maxDurationMs') {
      merged[key] = Math.max(Number(merged[key]) || 0, Number(value) || 0);
    } else {
      merged[key] = (Number(merged[key]) || 0) + (Number(value) || 0);
    }
  }
  return merged;
}

async function getSyncStats(options = {}) {
  const dates = buildDateRange(options.date, options.days);
  const aggregate = {
    timeZone: getTimeZone(),
    from: dates[0],
    to: dates[dates.length - 1],
    days: dates.length,
    totals: createEmptyCounts(),
    items: {},
  };

  for (const date of dates) {
    const stats = await readDailyStats(date);
    aggregate.totals = mergeCounts(aggregate.totals, stats.totals);
    for (const item of Object.values(stats.items || {})) {
      const existing = aggregate.items[item.key] || {
        ...item,
        counts: createEmptyCounts(),
        byDate: {},
      };
      existing.counts = mergeCounts(existing.counts, item.counts);
      existing.byDate[date] = item.counts;
      existing.lastSeenAt = !existing.lastSeenAt || item.lastSeenAt > existing.lastSeenAt
        ? item.lastSeenAt
        : existing.lastSeenAt;
      existing.lastTraceId = item.lastTraceId || existing.lastTraceId;
      existing.lastError = item.lastError || existing.lastError || '';
      aggregate.items[item.key] = existing;
    }
  }

  return {
    ...aggregate,
    items: Object.values(aggregate.items)
      .sort((a, b) => (b.counts.totalCalls || 0) - (a.counts.totalCalls || 0)),
  };
}

module.exports = {
  getSyncStats,
  recordSyncStat,
  getDateKey,
};
