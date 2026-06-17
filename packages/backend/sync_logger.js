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

function compactPayload(payload) {
  return Object.fromEntries(
    Object.entries(payload)
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
  );
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
  console[method](JSON.stringify(record));
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
