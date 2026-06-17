const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const CACHE_DIR = path.join(__dirname, '.sync_cache');
const CACHE_TTL_MS = 10 * 60 * 1000;

function buildCacheFilePath(taskId) {
  return path.join(CACHE_DIR, `${taskId}.json`);
}

function buildPageFilePath(taskId, offset) {
  return path.join(CACHE_DIR, `${taskId}_${offset}.page`);
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

async function saveTaskPage(taskId, offset, rows, hasMore) {
  await ensureCacheDir();
  const pagePath = buildPageFilePath(taskId, offset);
  const payload = {
    taskId,
    offset,
    rows: Array.isArray(rows) ? rows : [],
    hasMore: Boolean(hasMore),
    updatedAt: Date.now(),
  };
  await fsp.writeFile(pagePath, JSON.stringify(payload), 'utf8');
  return payload;
}

async function deleteTaskFiles(taskId) {
  await fsp.unlink(buildCacheFilePath(taskId)).catch(() => {});
  const files = await fsp.readdir(CACHE_DIR).catch(() => []);
  await Promise.all(
    files
      .filter((name) => name.startsWith(`${taskId}_`) && name.endsWith('.page'))
      .map((name) => fsp.unlink(path.join(CACHE_DIR, name)).catch(() => {}))
  );
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

async function createTask(querySignature = '', dataRows = null, options = {}) {
  const taskId = (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex')).replace(/-/g, '');
  const now = Date.now();
  const rows = Array.isArray(dataRows) ? dataRows : null;
  const pageSize = Math.max(parseInt(options.pageSize, 10) || 0, 0);
  const payload = {
    taskId,
    createdAt: now,
    expiresAt: nextExpiry(),
    querySignature,
    total: rows ? rows.length : 0,
    pageSize: pageSize || null,
    rows: rows && pageSize ? null : rows,
    pages: {},
  };

  if (rows && pageSize) {
    for (let offset = 0; offset < rows.length; offset += pageSize) {
      const pageRows = rows.slice(offset, offset + pageSize);
      const hasMore = offset + pageSize < rows.length;
      await saveTaskPage(taskId, offset, pageRows, hasMore);
      payload.pages[String(offset)] = {
        file: path.basename(buildPageFilePath(taskId, offset)),
        count: pageRows.length,
        hasMore,
        updatedAt: Date.now(),
      };
    }
  }

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
    await deleteTaskFiles(taskId);
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

async function getTaskPage(taskId, offset, querySignature = '', options = {}) {
  const payload = await loadTask(taskId, { querySignature, touch: false });
  const shouldTouchMetadata = !Array.isArray(payload.rows);
  const page = payload.pages && payload.pages[String(offset)];
  if (page && page.file) {
    const pagePayload = JSON.parse(await fsp.readFile(buildPageFilePath(taskId, offset), 'utf8'));
    if (shouldTouchMetadata) {
      payload.expiresAt = nextExpiry();
      await saveTask(taskId, payload);
    }
    return {
      payload,
      page: {
        rows: Array.isArray(pagePayload.rows) ? pagePayload.rows : [],
        hasMore: Boolean(pagePayload.hasMore),
        updatedAt: pagePayload.updatedAt,
      },
    };
  }
  if (page && Array.isArray(page.rows)) {
    if (shouldTouchMetadata) {
      payload.expiresAt = nextExpiry();
      await saveTask(taskId, payload);
    }
    return {
      payload,
      page,
    };
  }
  if (Array.isArray(payload.rows)) {
    const pageSize = Math.max(parseInt(options.pageSize || payload.pageSize, 10) || 1000, 1);
    const rows = payload.rows.slice(offset, offset + pageSize);
    return {
      payload,
      page: {
        rows,
        hasMore: offset + pageSize < payload.rows.length,
        updatedAt: payload.createdAt,
      },
    };
  }
  return {
    payload,
    page: null,
  };
}

async function setTaskPage(taskId, offset, rows, hasMore) {
  const payload = await loadTask(taskId, { touch: false });
  if (!payload.pages || typeof payload.pages !== 'object') {
    payload.pages = {};
  }
  const pagePayload = await saveTaskPage(taskId, offset, rows, hasMore);
  payload.pages[String(offset)] = {
    file: path.basename(buildPageFilePath(taskId, offset)),
    count: pagePayload.rows.length,
    hasMore: Boolean(hasMore),
    updatedAt: pagePayload.updatedAt,
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
            await deleteTaskFiles(payload?.taskId || path.basename(name, '.json'));
          }
        } catch (_) {
          await fsp.unlink(filePath).catch(() => {});
        }
      })
  );
}

async function getTaskFileSize(taskId) {
  const filePath = buildCacheFilePath(taskId);
  const stat = await fsp.stat(filePath);
  return stat.size;
}

module.exports = {
  CACHE_DIR,
  CACHE_TTL_MS,
  buildPageToken,
  parsePageToken,
  buildQuerySignature,
  createTask,
  loadTask,
  getTaskPage,
  setTaskPage,
  cleanupExpiredTasks,
  getTaskFileSize,
};
