const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const DEFAULT_TIME_ZONE = 'Asia/Shanghai';
const DEFAULT_DAYS = 7;
const MAX_DAYS = 90;
const MAX_PREVIEW_LENGTH = 180;
const MAX_SHAPE_DEPTH = 5;
const MAX_SHAPE_KEYS = 30;
const DEFAULT_SHAPE_LIMIT = 50;
const MAX_SHAPE_LIMIT = 500;

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

function buildUnknownShapePath(dateKey) {
  return path.join(getStatsDir(), `unknown_request_shapes-${dateKey}.jsonl`);
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

function isSensitiveKey(key) {
  const normalized = String(key || '').toLowerCase();
  return /(access|authorization|cookie|password|secret|signature|token|sql|query)/.test(normalized);
}

function getCandidateKind(key) {
  const normalized = String(key || '').toLowerCase().replace(/[_-]/g, '');
  if (normalized === 'tableid' || normalized === 'basetableid' || normalized === 'bitabletableid') return 'tableId';
  if (normalized === 'baseid' || normalized === 'bitableid') return 'baseId';
  if (normalized === 'viewid') return 'viewId';
  if (normalized === 'apptoken') return 'appToken';
  if (normalized === 'tenantkey') return 'tenantKey';
  return '';
}

function getValueType(value) {
  if (Array.isArray(value)) return 'array';
  if (value === null) return 'null';
  return typeof value;
}

function extractCandidateFields(value, pathPrefix = '', depth = 0, candidates = [], seen = new Set()) {
  if (value === undefined || value === null || depth > MAX_SHAPE_DEPTH) return candidates;
  let target = value;
  if (typeof target === 'string') {
    const parsed = parseJsonForShape(target);
    if (!parsed) return candidates;
    target = parsed;
  }
  if (typeof target !== 'object' || seen.has(target)) return candidates;
  seen.add(target);

  if (Array.isArray(target)) {
    target.slice(0, 3).forEach((item, index) => {
      extractCandidateFields(item, `${pathPrefix}[${index}]`, depth + 1, candidates, seen);
    });
    return candidates;
  }

  for (const [key, item] of Object.entries(target)) {
    const pathText = pathPrefix ? `${pathPrefix}.${key}` : key;
    const kind = getCandidateKind(key);
    if (kind) {
      const sensitive = isSensitiveKey(key);
      candidates.push({
        path: pathText,
        key,
        kind,
        type: getValueType(item),
        redacted: sensitive,
        valuePreview: sensitive ? '' : normalizeText(item, 120),
      });
    }
    extractCandidateFields(item, pathText, depth + 1, candidates, seen);
  }

  return candidates;
}

function parseJsonForShape(value) {
  if (typeof value !== 'string') return null;
  const text = value.trim();
  if (!text || (text[0] !== '{' && text[0] !== '[')) return null;
  try {
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function summarizeShape(value, depth = 0, seen = new Set()) {
  if (value === undefined) return { type: 'undefined' };
  if (value === null) return { type: 'null' };
  if (depth > MAX_SHAPE_DEPTH) return { type: getValueType(value), truncated: 'max_depth' };

  if (typeof value === 'string') {
    const parsed = parseJsonForShape(value);
    if (parsed) {
      return {
        type: 'json_string',
        parsed: summarizeShape(parsed, depth + 1, seen),
      };
    }
    return {
      type: 'string',
      length: value.length,
    };
  }

  if (typeof value !== 'object') {
    return { type: typeof value };
  }

  if (seen.has(value)) {
    return { type: getValueType(value), circular: true };
  }
  seen.add(value);

  if (Array.isArray(value)) {
    return {
      type: 'array',
      length: value.length,
      first: value.length > 0 ? summarizeShape(value[0], depth + 1, seen) : null,
    };
  }

  const entries = Object.entries(value);
  const children = {};
  for (const [key, item] of entries.slice(0, MAX_SHAPE_KEYS)) {
    children[key] = isSensitiveKey(key)
      ? { type: getValueType(item), redacted: true }
      : summarizeShape(item, depth + 1, seen);
  }
  return {
    type: 'object',
    keys: entries.map(([key]) => key).slice(0, MAX_SHAPE_KEYS),
    keyCount: entries.length,
    truncated: entries.length > MAX_SHAPE_KEYS,
    children,
  };
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

async function recordUnknownRequestShape(input = {}) {
  const tableId = input.context?.tableId || input.config?.tableId || 'unknown';
  if (tableId && tableId !== 'unknown') return;

  const calledAt = new Date();
  const dateKey = getDateKey(calledAt);
  const sourceInfo = getSourceInfo(input.config, input.context);
  const sample = {
    ts: calledAt.toISOString(),
    endpoint: input.endpoint || '',
    traceId: input.traceId || input.context?.traceId || '',
    source: sourceInfo.source,
    objectType: sourceInfo.objectType,
    objectName: sourceInfo.objectName,
    requestSource: input.requestSource || {},
    candidateFields: extractCandidateFields(input.requestBody || {}),
    bodyShape: summarizeShape(input.requestBody || {}),
  };

  return enqueueWrite(async () => {
    await ensureStatsDir();
    await fsp.appendFile(buildUnknownShapePath(dateKey), `${JSON.stringify(sample)}\n`, 'utf8');
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

async function getUnknownRequestShapes(options = {}) {
  const dates = buildDateRange(options.date, options.days || 1);
  const limit = Math.min(Math.max(parseInt(options.limit, 10) || DEFAULT_SHAPE_LIMIT, 1), MAX_SHAPE_LIMIT);
  const samples = [];

  for (const date of dates) {
    const filePath = buildUnknownShapePath(date);
    let content = '';
    try {
      content = await fsp.readFile(filePath, 'utf8');
    } catch (error) {
      if (!error || error.code !== 'ENOENT') {
        throw error;
      }
    }
    if (!content) continue;
    for (const line of content.split(/\r?\n/)) {
      if (!line.trim()) continue;
      try {
        samples.push(JSON.parse(line));
      } catch (_) {}
    }
  }

  return {
    timeZone: getTimeZone(),
    from: dates[0],
    to: dates[dates.length - 1],
    totalSamples: samples.length,
    samples: samples.slice(-limit).reverse(),
  };
}

module.exports = {
  getUnknownRequestShapes,
  getSyncStats,
  recordSyncStat,
  recordUnknownRequestShape,
  getDateKey,
};
