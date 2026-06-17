const { spawn } = require('child_process');
const path = require('path');

const PYODPS_SCRIPT_PATH = path.join(__dirname, 'pyodps_runner.py');
const PYODPS_COMMAND_TIMEOUT_MS = Math.max(
  parseInt(process.env.PYODPS_COMMAND_TIMEOUT_MS || '300000', 10) || 300000,
  10000
);

function formatLogValue(value) {
  return String(value).replace(/\s+/g, '_');
}

function logMaxCompute(event, details = {}, level = 'log') {
  const parts = Object.entries(details)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${key}=${formatLogValue(value)}`);
  console[level](`[maxcompute_client] event=${event}${parts.length ? ` ${parts.join(' ')}` : ''}`);
}

class PythonProcessManager {
  constructor() {
    this.process = null;
    this.queue = [];
    this.isProcessing = false;
    this.activeTask = null;
    this.nextRequestId = 1;
    this.startPromise = null;
    this.handlersBound = false;
    this.pythonCommand = process.platform === 'win32' ? 'python' : 'python3';
  }

  async ensureProcess() {
    if (this.process) return;
    if (!this.startPromise) {
      this.startPromise = this.startProcess().finally(() => {
        this.startPromise = null;
      });
    }
    await this.startPromise;
  }

  async startProcess() {
    return new Promise((resolve, reject) => {
      const proc = spawn(this.pythonCommand, [PYODPS_SCRIPT_PATH, '--daemon'], {
        cwd: __dirname,
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      let ready = false;
      let startupBuffer = '';
      let startupErr = '';

      const startupTimer = setTimeout(() => {
        if (!ready) {
          proc.kill();
          reject(new Error('Python process startup timeout'));
        }
      }, 10000);

      const onStartupStdout = (data) => {
        startupBuffer += data.toString();
        if (startupBuffer.includes('DAEMON_READY') && !ready) {
          ready = true;
          clearTimeout(startupTimer);
          proc.stdout.off('data', onStartupStdout);
          this.process = proc;
          this.handlersBound = false;
          this.bindProcessHandlers();
          resolve();
        }
      };

      proc.stdout.on('data', onStartupStdout);
      proc.stderr.on('data', (data) => {
        const chunk = data.toString();
        startupErr += chunk;
        logMaxCompute('python_stderr_startup', { bytes: chunk.length }, 'error');
      });
      proc.on('close', (code) => {
        this.process = null;
        this.handlersBound = false;
        if (!ready) {
          clearTimeout(startupTimer);
          reject(new Error(`Python process exited before ready, code=${code}, stderr=${startupErr}`));
        }
      });
    });
  }

  bindProcessHandlers() {
    if (!this.process || this.handlersBound) return;
    this.handlersBound = true;

    let buffer = '';

    this.process.stdout.on('data', (data) => {
      buffer += data.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line || line === 'DAEMON_READY') continue;
        this.handleResponse(line);
      }
    });

    this.process.stderr.on('data', (data) => {
      const chunk = data.toString();
      logMaxCompute('python_stderr', {
        traceId: this.activeTask?.command?.trace_id,
        requestId: this.activeTask?.requestId,
        action: this.activeTask?.command?.action,
        bytes: chunk.length,
      }, 'error');
    });

    this.process.on('close', () => {
      this.process = null;
      this.handlersBound = false;
      if (this.activeTask) {
        const task = this.activeTask;
        this.activeTask = null;
        this.isProcessing = false;
        this.removeTask(task);
        task.resolve({ success: false, message: 'Python process closed unexpectedly' });
      }
      this.processQueue();
    });
  }

  removeTask(task) {
    const index = this.queue.indexOf(task);
    if (index > -1) this.queue.splice(index, 1);
  }

  sendCommand(command) {
    if (!this.process) {
      throw new Error('Python process is not running');
    }
    this.process.stdin.write(JSON.stringify(command) + '\n');
  }

  handleResponse(response) {
    const trimmed = String(response || '').trim();
    if (!trimmed || (trimmed[0] !== '{' && trimmed[0] !== '[')) {
      return;
    }

    let result;
    try {
      result = JSON.parse(trimmed);
    } catch {
      logMaxCompute('python_json_parse_error', {
        traceId: this.activeTask?.command?.trace_id,
        requestId: this.activeTask?.requestId,
        action: this.activeTask?.command?.action,
        bytes: trimmed.length,
      }, 'error');
      if (this.activeTask) {
        const task = this.activeTask;
        this.activeTask = null;
        this.isProcessing = false;
        this.removeTask(task);
        task.resolve({ success: false, message: 'Invalid Python output' });
      }
      this.processQueue();
      return;
    }

    if (!this.activeTask) return;
    if (result.request_id !== undefined && String(result.request_id) !== String(this.activeTask.requestId)) {
      return;
    }

    const task = this.activeTask;
    this.activeTask = null;
    this.isProcessing = false;
    this.removeTask(task);
    delete result.request_id;
    task.resolve(result);
    this.processQueue();
  }

  processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;

    const task = this.queue[0];
    this.activeTask = task;
    this.isProcessing = true;

    this.ensureProcess()
      .then(() => this.sendCommand({ ...task.command, request_id: task.requestId }))
      .catch((error) => {
        this.activeTask = null;
        this.isProcessing = false;
        this.removeTask(task);
        task.resolve({ success: false, message: error.message });
        this.processQueue();
      });
  }

  async runCommand(command) {
    return new Promise((resolve) => {
      let timeoutId;
      const task = {
        requestId: this.nextRequestId++,
        command,
        settled: false,
        resolve: (result) => {
          if (task.settled) return;
          task.settled = true;
          clearTimeout(timeoutId);
          resolve(result);
        },
      };

      this.queue.push(task);
      timeoutId = setTimeout(() => {
        const wasActive = this.activeTask === task;
        this.removeTask(task);
        if (wasActive) {
          this.activeTask = null;
          this.isProcessing = false;
        }
        task.resolve({ success: false, message: 'Operation timed out' });
        if (wasActive) this.processQueue();
      }, PYODPS_COMMAND_TIMEOUT_MS);

      this.processQueue();
    });
  }

  close() {
    if (this.process) {
      this.process.kill();
      this.process = null;
      this.handlersBound = false;
    }
  }
}

const pythonProcessManager = new PythonProcessManager();
const MAX_RETRY_COUNT = 2;
const RETRY_DELAY_MS = 1000;

async function runPyOdps(action, config) {
  let lastError = null;
  const traceId = config.traceId || '';
  
  for (let retry = 0; retry <= MAX_RETRY_COUNT; retry++) {
    const startTime = Date.now();
    
    if (retry > 0) {
      logMaxCompute('retry_start', { traceId, action, attempt: retry, endpoint: config.endpoint, project: config.projectName });
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * retry));
    } else {
      logMaxCompute('start', { traceId, action, endpoint: config.endpoint, project: config.projectName });
    }
    
    try {
      const result = await pythonProcessManager.runCommand({
        action,
        trace_id: traceId,
        endpoint: config.endpoint,
        project: config.projectName,
        access_id: config.accessId,
        access_key: config.accessKey,
        table_name: config.tableName || '',
        sql: config.sql || '',
        limit: config.limit,
        offset: config.offset || 0,
        cache_dir: config.cacheDir || '',
        query_signature: config.querySignature || '',
        page_size: config.pageSize,
        task_id: config.taskId || '',
      });
      
      const duration = Date.now() - startTime;
      logMaxCompute('done', {
        traceId,
        action,
        success: result.success,
        durationMs: duration,
        taskId: result.taskId,
        total: result.total,
        executeMs: result.timing?.executeMs,
        waitMs: result.timing?.waitMs,
        readWriteMs: result.timing?.readWriteMs,
      });
      
      if (result.success) {
        return result;
      }
      
      lastError = new Error(result.message);
      if (result.message && result.message.includes('timed out')) {
        logMaxCompute('retry_scheduled', { traceId, action, attempt: retry + 1, reason: result.message });
        continue;
      }
      throw lastError;
      
    } catch (error) {
      lastError = error;
      logMaxCompute('failed', { traceId, action, attempt: retry, message: error.message }, 'error');
      if (retry < MAX_RETRY_COUNT && (error.message.includes('timed out') || error.message.includes('timeout'))) {
        logMaxCompute('retry_scheduled', { traceId, action, attempt: retry + 1, reason: error.message });
        continue;
      }
      throw lastError;
    }
  }
  
  throw lastError || new Error(`${action} 执行失败，已重试 ${MAX_RETRY_COUNT} 次`);
}

process.on('exit', () => {
  pythonProcessManager.close();
});

class MaxComputeClient {
  constructor(config) {
    this.accessId = config.accessId;
    this.accessKey = config.accessKey;
    this.endpoint = config.endpoint;
    this.projectName = config.projectName;
    this.schemaName = config.schemaName || 'default';
    this.region = config.region || this._extractRegionFromEndpoint(config.endpoint);
    this.traceId = config.traceId || '';
  }

  _extractRegionFromEndpoint(endpoint) {
    if (!endpoint) return 'cn-hangzhou';
    const match = endpoint.match(/service\.([a-z0-9-]+)\.maxcompute/);
    return match ? match[1].replace('-vpc', '').replace('-intranet', '') : 'cn-hangzhou';
  }

  async testConnection() {
    const result = await runPyOdps('test_connection', this);
    if (result.success) {
      return {
        success: true,
        message: '连接成功',
        data: {
          projectName: this.projectName,
          schemaName: this.schemaName,
          endpoint: this.endpoint,
          region: this.region,
        },
      };
    }
    return { success: false, message: result.message || '连接失败' };
  }

  async getTables() {
    const result = await runPyOdps('get_tables', this);
    if (!result.success) throw new Error(result.message || '获取表列表失败');
    return result.data || [];
  }

  async getTableMeta(tableName) {
    const result = await runPyOdps('get_table_meta', { ...this, tableName });
    if (!result.success) throw new Error(result.message || '获取表元数据失败');
    return result.data;
  }

  async getTableData(tableName, limit = null, offset = 0) {
    const result = await runPyOdps('get_table_data', { ...this, tableName, limit, offset });
    if (!result.success) throw new Error(result.message || '获取表数据失败');
    return result.data || [];
  }

  _buildLimitedSQL(sql, limit = null, offset = 0) {
    let finalSQL = String(sql || '').trim().replace(/;+\s*$/, '');
    const hasLimitClause = /\blimit\s+\d+(\s+offset\s+\d+)?\s*$/i.test(finalSQL);
    if (limit !== null && !hasLimitClause) {
      finalSQL = `${finalSQL} LIMIT ${limit} OFFSET ${offset}`;
    }
    return finalSQL;
  }

  async executeSQL(sql, limit = null, offset = 0) {
    const finalSQL = this._buildLimitedSQL(sql, limit, offset);
    const result = await runPyOdps('execute_sql', { ...this, sql: finalSQL, limit, offset });
    if (!result.success) throw new Error(result.message || '执行 SQL 失败');
    return result.data || [];
  }

  async executeSQLToCache(sql, cacheDir, querySignature, pageSize = 1000, taskId = '') {
    const finalSQL = this._buildLimitedSQL(sql, null, 0);
    const result = await runPyOdps('execute_sql_to_cache', {
      ...this,
      sql: finalSQL,
      cacheDir,
      querySignature,
      pageSize,
      taskId,
    });
    if (!result.success) throw new Error(result.message || '执行 SQL 写缓存失败');
    return result;
  }

  async getQueryMeta(sql) {
    const finalSQL = this._buildLimitedSQL(sql, 1, 0);
    const result = await runPyOdps('execute_sql', { ...this, sql: finalSQL, limit: 1, offset: 0 });
    if (!result.success) throw new Error(result.message || '获取 SQL 字段失败');
    const columns = Array.isArray(result.columns) ? result.columns : [];
    if (columns.length === 0) {
      throw new Error('自定义 SQL 未返回字段信息');
    }
    return {
      Table: {
        Name: 'custom_query',
        Columns: columns,
      },
    };
  }

  async close() {}
}

const ENDPOINT_CONFIG = {
  public: {
    'cn-hangzhou': 'https://service.cn-hangzhou.maxcompute.aliyun.com/api',
    'cn-shanghai': 'https://service.cn-shanghai.maxcompute.aliyun.com/api',
    'cn-beijing': 'https://service.cn-beijing.maxcompute.aliyun.com/api',
    'cn-zhangjiakou': 'https://service.cn-zhangjiakou.maxcompute.aliyun.com/api',
    'cn-wulanchabu': 'https://service.cn-wulanchabu.maxcompute.aliyun.com/api',
    'cn-shenzhen': 'https://service.cn-shenzhen.maxcompute.aliyun.com/api',
    'cn-chengdu': 'https://service.cn-chengdu.maxcompute.aliyun.com/api',
    'cn-hongkong': 'https://service.cn-hongkong.maxcompute.aliyun.com/api',
    'ap-southeast-1': 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    'ap-northeast-1': 'https://service.ap-northeast-1.maxcompute.aliyun.com/api',
    'eu-central-1': 'https://service.eu-central-1.maxcompute.aliyun.com/api',
    'us-west-1': 'https://service.us-west-1.maxcompute.aliyun.com/api',
    'us-east-1': 'https://service.us-east-1.maxcompute.aliyun.com/api',
  },
  vpc: {
    'cn-hangzhou': 'https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api',
    'cn-shanghai': 'https://service.cn-shanghai-vpc.maxcompute.aliyun-inc.com/api',
    'cn-beijing': 'https://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api',
  },
  intranet: {
    'cn-hangzhou': 'https://service.cn-hangzhou-intranet.maxcompute.aliyun-inc.com/api',
    'cn-shanghai': 'https://service.cn-shanghai-intranet.maxcompute.aliyun-inc.com/api',
    'cn-beijing': 'https://service.cn-beijing-intranet.maxcompute.aliyun-inc.com/api',
  },
};

function getEndpoint(region, networkType = 'public') {
  const endpoints = ENDPOINT_CONFIG[networkType];
  if (!endpoints) {
    throw new Error(`Unsupported network type: ${networkType}`);
  }
  const endpoint = endpoints[region];
  if (!endpoint) {
    throw new Error(`Unsupported region: ${region}`);
  }
  return endpoint;
}

module.exports = { MaxComputeClient, getEndpoint, ENDPOINT_CONFIG };
