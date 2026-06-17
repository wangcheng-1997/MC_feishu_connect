const LEVEL_PRIORITY = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
};

function normalizeLevel(level) {
  if (!level || level === 'log') return 'info';
  return LEVEL_PRIORITY[level] === undefined ? 'info' : level;
}

function getLogLevel() {
  return normalizeLevel(String(process.env.LOG_LEVEL || 'info').toLowerCase());
}

function shouldLog(level) {
  return LEVEL_PRIORITY[normalizeLevel(level)] <= LEVEL_PRIORITY[getLogLevel()];
}

function isJsonLogFormat() {
  const format = String(process.env.LOG_FORMAT || process.env.SYNC_LOG_FORMAT || 'pretty').toLowerCase();
  return format === 'json' || format === 'jsonl';
}

function compactPayload(payload) {
  return Object.fromEntries(
    Object.entries(payload)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
  );
}

function formatLogValue(value) {
  return String(value).replace(/\s+/g, '_');
}

function formatMs(value) {
  const ms = Number(value);
  if (!Number.isFinite(ms)) return '';
  return ms === 0 ? '<1ms' : `${ms}ms`;
}

function formatShortId(value, length = 8) {
  const text = String(value || '');
  return text.length > length ? text.slice(0, length) : text;
}

function formatTtl(value) {
  const ms = Number(value);
  if (!Number.isFinite(ms)) return '';
  return `${Math.round(ms / 1000)}s`;
}

function pickHumanDetails(record) {
  const details = [];
  const used = new Set([
    'ts',
    'level',
    'event',
    'traceId',
    'tableId',
    'source',
    'objectType',
    'objectName',
    'stage',
    'stageName',
    'stageMs',
    'elapsedMs',
    'durationMs',
  ]);

  const add = (key, value, label = key, formatter = formatLogValue) => {
    used.add(key);
    if (value === undefined || value === null || value === '') return;
    details.push(`${label}=${formatter(value)}`);
  };

  add('taskId', record.taskId, 'task', (value) => formatShortId(value, 8));
  add('pageOffset', record.pageOffset, 'page');
  add('pageSize', record.pageSize, 'size');
  add('total', record.total);
  add('hasMore', record.hasMore);
  add('nextOffset', record.nextOffset, 'next');
  add('expiresInMs', record.expiresInMs, 'ttl', formatTtl);
  add('fieldSource', record.fieldSource, 'fields');
  add('limit', record.limit);
  add('offset', record.offset);
  add('hasPageToken', record.hasPageToken, 'token');
  add('cacheFileBytes', record.cacheFileBytes, 'cacheBytes');

  if (
    record.executeMs !== undefined ||
    record.waitMs !== undefined ||
    record.readWriteMs !== undefined
  ) {
    used.add('executeMs');
    used.add('waitMs');
    used.add('readWriteMs');
    const executeMs = record.executeMs ?? '-';
    const waitMs = record.waitMs ?? '-';
    const readWriteMs = record.readWriteMs ?? '-';
    details.push(`mc=${executeMs}/${waitMs}/${readWriteMs}ms`);
  }

  for (const [key, value] of Object.entries(record)) {
    if (used.has(key) || value === undefined || value === null || value === '') continue;
    details.push(`${key}=${formatLogValue(value)}`);
  }

  return details.join(' ');
}

function formatSyncLine(record) {
  const prefix = record.event === 'sync.failed' ? '[sync failed]' : '[sync]';
  const identity = [
    record.traceId ? `trace=${formatShortId(record.traceId, 12)}` : '',
    `table=${formatLogValue(record.tableId || 'unknown')}`,
    record.source && record.objectType ? `${record.source}/${record.objectType}` : record.source,
    record.objectName ? formatLogValue(record.objectName) : '',
  ].filter(Boolean).join(' ');
  const stage = [record.stage, record.stageName].filter(Boolean).join(' ');
  const timing = [
    record.stageMs !== undefined ? `+${formatMs(record.stageMs)}` : '',
    record.elapsedMs !== undefined ? `elapsed=${formatMs(record.elapsedMs)}` : '',
  ].filter(Boolean).join(' ');
  const details = pickHumanDetails(record);
  return [prefix, identity, stage, timing, details]
    .filter(Boolean)
    .join(' | ');
}

function formatAppLine(record) {
  const [scope, ...eventParts] = String(record.event || 'app.log').split('.');
  const eventName = eventParts.join('.') || 'log';
  const identity = [
    record.traceId ? `trace=${formatShortId(record.traceId, 12)}` : '',
    record.tableId ? `table=${formatLogValue(record.tableId)}` : '',
    record.source && record.objectType ? `${record.source}/${record.objectType}` : record.source,
    record.objectName ? formatLogValue(record.objectName) : '',
  ].filter(Boolean).join(' ');
  const timing = record.durationMs !== undefined ? `duration=${formatMs(record.durationMs)}` : '';
  const details = pickHumanDetails(record);
  return [`[${scope}]`, eventName, identity, timing, details]
    .filter(Boolean)
    .join(' | ');
}

function writeLog(level, payload) {
  const normalizedLevel = normalizeLevel(level);
  if (!shouldLog(normalizedLevel)) return;
  const record = compactPayload({
    ts: new Date().toISOString(),
    level: normalizedLevel,
    ...payload,
  });
  const method = normalizedLevel === 'error' ? 'error' : normalizedLevel === 'warn' ? 'warn' : 'log';
  if (isJsonLogFormat()) {
    console[method](JSON.stringify(record));
    return;
  }
  const line = String(record.event || '').startsWith('sync.')
    ? formatSyncLine(record)
    : formatAppLine(record);
  console[method](line);
}

function logAppEvent(scope, event, details = {}, level = 'info') {
  writeLog(level, {
    event: `${scope}.${event}`,
    ...details,
  });
}

function logSyncEvent(context = {}, details = {}, level = 'info') {
  const stageName = details.stageName || details.step;
  const stageMs = details.stageMs ?? details.stageDurationMs;
  const elapsedMs = details.elapsedMs ?? details.totalDurationMs;
  const { step, stageDurationMs, totalDurationMs, ...rest } = details;
  writeLog(level, {
    event: details.event || 'sync.stage',
    traceId: context.traceId,
    tableId: context.tableId || 'unknown',
    source: context.source,
    objectType: context.object,
    objectName: context.sourceName,
    stageName,
    stageMs,
    elapsedMs,
    ...rest,
  });
}

function createSyncStageLogger(context, totalStages) {
  let lastAt = Date.now();
  const startedAt = lastAt;
  let isFirst = true;

  return function logStage(stageIndex, stageName, details = {}, level = 'info') {
    const now = Date.now();
    const stageMs = isFirst ? 0 : now - lastAt;
    isFirst = false;
    lastAt = now;
    logSyncEvent(context, {
      stage: `${stageIndex}/${totalStages}`,
      stageName,
      stageMs,
      elapsedMs: now - startedAt,
      ...details,
    }, level);
  };
}

function logSyncFailure(context, details = {}) {
  logSyncEvent(context, {
    event: 'sync.failed',
    stage: 'failed',
    stageName: 'failed',
    ...details,
  }, 'error');
}

function isDebugEnabled() {
  return shouldLog('debug');
}

module.exports = {
  createSyncStageLogger,
  isDebugEnabled,
  logAppEvent,
  logSyncEvent,
  logSyncFailure,
};
