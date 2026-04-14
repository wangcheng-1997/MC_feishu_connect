const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const CACHE_DIR = path.join(__dirname, '.sync_cache');
const CACHE_TTL_MS = 10 * 60 * 1000;

function buildCacheFilePath(taskId) {
  return path.join(CACHE_DIR, `${taskId}.json`);
}

async function ensureCacheDir() {
  if (!fs.existsSync(CACHE_DIR)) {
    await fsp.mkdir(CACHE_DIR, { recursive: true });
  }
}

function parsePageToken(pageToken) {
  if (!pageToken || typeof pageToken !== 'string') return null;
  const match = pageToken.match(/^([a-zA-Z0-9_-]+):(\d+)$/);
  if (!match) return null;
  return {
    taskId: match[1],
    offset: parseInt(match[2], 10),
  };
}

function buildPageToken(taskId, offset) {
  return `${taskId}:${offset}`;
}

async function saveTask(taskId, payload) {
  await ensureCacheDir();
  const filePath = buildCacheFilePath(taskId);
  await fsp.writeFile(filePath, JSON.stringify(payload), 'utf8');
  return filePath;
}

function nextExpiry() {
  return Date.now() + CACHE_TTL_MS;
}

function buildQuerySignature(config = {}) {
  const base = {
    endpoint: config.endpoint || '',
    projectName: config.projectName || '',
    tableName: config.tableName || '',
    sql: config.sql || '',
  };
  return crypto.createHash('sha1').update(JSON.stringify(base)).digest('hex');
}

async function createTask(querySignature = '') {
  const taskId = (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex')).replace(/-/g, '');
  const now = Date.now();
  const payload = {
    taskId,
    createdAt: now,
    expiresAt: nextExpiry(),
    querySignature,
    pages: {},
  };
  await saveTask(taskId, payload);
  return payload;
}

async function loadTask(taskId, options = {}) {
  const filePath = buildCacheFilePath(taskId);
  const content = await fsp.readFile(filePath, 'utf8');
  const payload = JSON.parse(content);
  if (!payload || payload.taskId !== taskId) {
    throw new Error('Invalid cache payload');
  }
  if (payload.expiresAt <= Date.now()) {
    await fsp.unlink(filePath).catch(() => {});
    throw new Error('Cache expired');
  }
  if (options.querySignature && payload.querySignature && options.querySignature !== payload.querySignature) {
    throw new Error('Cache signature mismatch');
  }
  if (options.touch !== false) {
    payload.expiresAt = nextExpiry();
    await saveTask(taskId, payload);
  }
  return payload;
}

async function getTaskPage(taskId, offset, querySignature = '') {
  const payload = await loadTask(taskId, { querySignature, touch: true });
  const page = payload.pages && payload.pages[String(offset)];
  return {
    payload,
    page: page || null,
  };
}

async function setTaskPage(taskId, offset, rows, hasMore) {
  const payload = await loadTask(taskId, { touch: false });
  if (!payload.pages || typeof payload.pages !== 'object') {
    payload.pages = {};
  }
  payload.pages[String(offset)] = {
    rows: Array.isArray(rows) ? rows : [],
    hasMore: Boolean(hasMore),
    updatedAt: Date.now(),
  };
  payload.expiresAt = nextExpiry();
  await saveTask(taskId, payload);
  return payload.pages[String(offset)];
}

async function cleanupExpiredTasks() {
  await ensureCacheDir();
  const files = await fsp.readdir(CACHE_DIR);
  await Promise.all(
    files
      .filter((name) => name.endsWith('.json'))
      .map(async (name) => {
        const filePath = path.join(CACHE_DIR, name);
        try {
          const content = await fsp.readFile(filePath, 'utf8');
          const payload = JSON.parse(content);
          if (!payload || !payload.expiresAt || payload.expiresAt <= Date.now()) {
            await fsp.unlink(filePath);
          }
        } catch (_) {}
      })
  );
}

module.exports = {
  CACHE_TTL_MS,
  buildPageToken,
  parsePageToken,
  buildQuerySignature,
  createTask,
  loadTask,
  getTaskPage,
  setTaskPage,
  cleanupExpiredTasks,
};
