const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const PYODPS_SCRIPT = `
import sys
import json
import argparse
import datetime

connection_pool = {}

def get_connection(endpoint, project_name, access_id, access_key):
    try:
        key = f"{endpoint}:{project_name}:{access_id}:{access_key}"
        if key in connection_pool:
            return connection_pool[key]
        
        from odps import ODPS
        
        odps = ODPS(
            access_id=access_id,
            secret_access_key=access_key,
            project=project_name,
            endpoint=endpoint
        )
        
        connection_pool[key] = odps
        return odps
    except ImportError as e:
        raise Exception(f"PyODPS 导入失败: {str(e)}，请安装: pip install pyodps")
    except Exception as e:
        raise Exception(f"创建连接失败: {str(e)}")

def test_connection(endpoint, project_name, access_id, access_key):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        project = odps.get_project()
        return {'success': True, 'message': '连接成功', 'data': {'projectName': project.name}}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def get_tables(endpoint, project_name, access_id, access_key):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        tables = list(odps.list_tables())
        return {'success': True, 'data': [{'name': t.name, 'schema': 'default'} for t in tables]}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def get_table_meta(endpoint, project_name, access_id, access_key, table_name):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        table = odps.get_table(table_name)
        
        columns = []
        for col in table.table_schema.columns:
            columns.append({
                'Name': col.name,
                'Type': str(col.type),
                'Comment': col.comment or ''
            })
        
        return {
            'success': True,
            'data': {
                'Table': {
                    'Name': table.name,
                    'Columns': columns
                }
            }
        }
    except Exception as e:
        return {'success': False, 'message': str(e)}

def execute_sql(endpoint, project_name, access_id, access_key, sql, limit=None):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        
        instance = odps.execute_sql(sql)
        instance.wait_for_completion()
        
        with instance.open_reader() as reader:
            records = []
            row_count = 0
            schema = reader.schema
            for record in reader:
                if limit is not None and row_count >= limit:
                    break
                record_dict = {}
                for i, col in enumerate(schema.columns):
                    val = record[i]
                    if val is None:
                        record_dict[col.name] = None
                    elif isinstance(val, (datetime.datetime, datetime.date)):
                        record_dict[col.name] = val.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        record_dict[col.name] = str(val)
                records.append(record_dict)
                row_count += 1
        
        return {'success': True, 'data': records, 'total': row_count}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def get_table_data(endpoint, project_name, access_id, access_key, table_name, limit=None, offset=0):
    try:
        sql = f"SELECT * FROM {table_name}"
        if limit is not None:
            sql = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
        return execute_sql(endpoint, project_name, access_id, access_key, sql, limit)
    except Exception as e:
        return {'success': False, 'message': str(e)}

def handle_command(command):
    request_id = command.get('request_id')
    try:
        action = command.get('action')
        endpoint = command.get('endpoint')
        project = command.get('project')
        access_id = command.get('access_id')
        access_key = command.get('access_key')
        table_name = command.get('table_name', '')
        sql = command.get('sql', '')
        limit = command.get('limit')
        offset = command.get('offset', 0)
        
        if action == 'test_connection':
            result = test_connection(endpoint, project, access_id, access_key)
        elif action == 'get_tables':
            result = get_tables(endpoint, project, access_id, access_key)
        elif action == 'get_table_meta':
            result = get_table_meta(endpoint, project, access_id, access_key, table_name)
        elif action == 'execute_sql':
            result = execute_sql(endpoint, project, access_id, access_key, sql, limit)
        elif action == 'get_table_data':
            result = get_table_data(endpoint, project, access_id, access_key, table_name, limit, offset)
        else:
            result = {'success': False, 'message': f'未知操作: {action}'}
        
        if request_id is not None:
            result['request_id'] = request_id
        return result
    except Exception as e:
        result = {'success': False, 'message': str(e)}
        if request_id is not None:
            result['request_id'] = request_id
        return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', action='store_true', help='Run in daemon mode')
    parser.add_argument('action', nargs='?', default='')
    parser.add_argument('--endpoint', required=False)
    parser.add_argument('--project', required=False)
    parser.add_argument('--access_id', required=False)
    parser.add_argument('--access_key', required=False)
    parser.add_argument('--table_name', default='')
    parser.add_argument('--sql', default='')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--offset', type=int, default=0)
    
    args = parser.parse_args()
    
    if args.daemon:
        print('DAEMON_READY', flush=True)
        
        while True:
            try:
                line = sys.stdin.readline()
                if not line:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                command = json.loads(line)
                result = handle_command(command)
                print(json.dumps(result), flush=True)
            except json.JSONDecodeError:
                print(json.dumps({'success': False, 'message': '无效的 JSON 命令'}), flush=True)
            except Exception as e:
                print(json.dumps({'success': False, 'message': str(e)}), flush=True)
    else:
        try:
            if args.action == 'test_connection':
                result = test_connection(args.endpoint, args.project, args.access_id, args.access_key)
            elif args.action == 'get_tables':
                result = get_tables(args.endpoint, args.project, args.access_id, args.access_key)
            elif args.action == 'get_table_meta':
                result = get_table_meta(args.endpoint, args.project, args.access_id, args.access_key, args.table_name)
            elif args.action == 'execute_sql':
                result = execute_sql(args.endpoint, args.project, args.access_id, args.access_key, args.sql, args.limit)
            elif args.action == 'get_table_data':
                result = get_table_data(args.endpoint, args.project, args.access_id, args.access_key, args.table_name, args.limit, args.offset)
            else:
                result = {'success': False, 'message': f'未知操作: {args.action}'}
            
            print(json.dumps(result), file=sys.stdout)
            sys.stdout.flush()
        except Exception as e:
            print(json.dumps({'success': False, 'message': str(e)}), file=sys.stdout)
            sys.stdout.flush()
`;

const PYODPS_SCRIPT_PATH = path.join(__dirname, 'pyodps_runner.py');
const PYODPS_COMMAND_TIMEOUT_MS = Math.max(
  parseInt(process.env.PYODPS_COMMAND_TIMEOUT_MS || '120000', 10) || 120000,
  10000
);

if (!fs.existsSync(PYODPS_SCRIPT_PATH)) {
  fs.writeFileSync(PYODPS_SCRIPT_PATH, PYODPS_SCRIPT);
}

// Python 进程管理类
class PythonProcessManager {
  constructor() {
    this.process = null;
    this.queue = [];
    this.isProcessing = false;
    this.activeTask = null;
    this.nextRequestId = 1;
    this.pythonCommand = this._detectPythonCommand();
  }

  _detectPythonCommand() {
    if (process.env.REPLIT) return 'python3.11';
    return process.platform === 'win32' ? 'python' : 'python3';
  }

  _startProcess() {
    return new Promise((resolve, reject) => {
      console.log('启动 Python 进程...');
      
      const proc = spawn(this.pythonCommand, [PYODPS_SCRIPT_PATH, '--daemon'], {
        cwd: __dirname,
        stdio: ['pipe', 'pipe', 'pipe']
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => {
        stdout += data.toString();
        if (stdout.includes('DAEMON_READY')) {
          this.process = proc;
          this._setupProcessHandlers();
          resolve(true);
        }
      });

      proc.stderr.on('data', (data) => {
        stderr += data.toString();
        console.error('Python 错误:', data.toString());
      });

      proc.on('close', (code) => {
        console.log('Python 进程关闭，退出码:', code);
        this.process = null;
        if (!stdout.includes('DAEMON_READY')) {
          reject(new Error(`Python 进程启动失败: ${stderr}`));
        }
      });

      // 超时处理
      setTimeout(() => {
        if (!this.process) {
          proc.kill();
          reject(new Error('Python 进程启动超时'));
        }
      }, 10000);
    });
  }

  _setupProcessHandlers() {
    if (!this.process) return;

    let buffer = '';

    this.process.stdout.on('data', (data) => {
      buffer += data.toString();
      const lines = buffer.split('\n');
      
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i].trim();
        if (line) {
          this._handleResponse(line);
        }
      }
      
      buffer = lines[lines.length - 1];
    });

    this.process.stderr.on('data', (data) => {
      console.error('Python 错误输出:', data.toString());
    });

    this.process.on('close', (code) => {
      console.log('Python 进程关闭，退出码:', code);
      this.process = null;
      if (this.activeTask) {
        const task = this.activeTask;
        this.activeTask = null;
        this.isProcessing = false;
        const index = this.queue.indexOf(task);
        if (index > -1) {
          this.queue.splice(index, 1);
        }
        task.resolve({ success: false, message: 'Python process closed unexpectedly' });
      }
      this._processQueue();
    });
  }

  _handleResponse(response) {
    try {
      const result = JSON.parse(response);
      const task = this.queue.shift();
      if (task) {
        task.resolve(result);
      }
      this.isProcessing = false;
      this._processQueue();
    } catch (e) {
      console.error('JSON 解析错误:', response);
      const task = this.queue.shift();
      if (task) {
        task.resolve({ success: false, message: `无法解析输出: ${response}` });
      }
      this.isProcessing = false;
      this._processQueue();
    }
  }

  async _ensureProcess() {
    if (!this.process) {
      await this._startProcess();
    }
  }

  _processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;

    this.isProcessing = true;
    const task = this.queue[0];
    this.activeTask = task;
    task.sent = true;
    this._sendCommand({
      ...task.command,
      request_id: task.requestId,
    });
  }

  _sendCommand(command) {
    if (!this.process) {
      const task = this.queue.shift();
      if (task) {
        task.resolve({ success: false, message: 'Python 进程未运行' });
      }
      this.isProcessing = false;
      this._processQueue();
      return;
    }

    try {
      this.process.stdin.write(JSON.stringify(command) + '\n');
    } catch (error) {
      console.error('发送命令失败:', error);
      this.process = null;
      const task = this.queue.shift();
      if (task) {
        task.resolve({ success: false, message: '发送命令失败' });
      }
      this.isProcessing = false;
      this._processQueue();
    }
  }

  async runCommand(command) {
    return new Promise(async (resolve) => {
      // 创建任务
      const task = {
        requestId: this.nextRequestId++,
        command,
        settled: false,
        resolve: (result) => {
          if (task.settled) {
            return;
          }
          task.settled = true;
          clearTimeout(timeoutId);
          resolve(result);
        }
      };

      this.queue.push(task);

      // 超时定时器
      const timeoutId = setTimeout(() => {
        // 从队列中移除任务
        const index = this.queue.indexOf(task);
        if (index > -1) {
          this.queue.splice(index, 1);
        }
        resolve({ success: false, message: '操作超时' });
      }, PYODPS_COMMAND_TIMEOUT_MS);

      try {
        await this._ensureProcess();
        this._processQueue();
      } catch (error) {
        console.error('启动进程失败:', error);
        // 从队列中移除任务
        const index = this.queue.indexOf(task);
        if (index > -1) {
          this.queue.splice(index, 1);
        }
        clearTimeout(timeoutId);
        resolve({ success: false, message: error.message });
      }
    });
  }

  _removeTask(task) {
    const index = this.queue.indexOf(task);
    if (index > -1) {
      this.queue.splice(index, 1);
    }
  }

  _sendCommand(command) {
    if (!this.process) {
      throw new Error('Python process is not running');
    }

    try {
      this.process.stdin.write(JSON.stringify(command) + '\n');
    } catch (error) {
      this.process = null;
      throw new Error('Failed to send command to Python process');
    }
  }

  _handleResponse(response) {
    let result;

    try {
      result = JSON.parse(response);
    } catch (error) {
      console.error('JSON parse error:', response);
      if (this.activeTask) {
        const task = this.activeTask;
        this.activeTask = null;
        this.isProcessing = false;
        this._removeTask(task);
        task.resolve({ success: false, message: `Invalid Python output: ${response}` });
      }
      this._processQueue();
      return;
    }

    if (!this.activeTask) {
      return;
    }

    const responseId = result.request_id;
    if (responseId !== undefined && String(responseId) !== String(this.activeTask.requestId)) {
      return;
    }

    const task = this.activeTask;
    this.activeTask = null;
    this.isProcessing = false;
    this._removeTask(task);
    delete result.request_id;
    task.resolve(result);
    this._processQueue();
  }

  _processQueue() {
    if (this.isProcessing || this.queue.length === 0) return;

    const task = this.queue[0];
    this.activeTask = task;
    this.isProcessing = true;
    task.sent = true;

    this._ensureProcess()
      .then(() => {
        this._sendCommand({
          ...task.command,
          request_id: task.requestId,
        });
      })
      .catch((error) => {
        this.activeTask = null;
        this.isProcessing = false;
        this._removeTask(task);
        task.resolve({ success: false, message: error.message });
        this._processQueue();
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
          if (task.settled) {
            return;
          }
          task.settled = true;
          clearTimeout(timeoutId);
          resolve(result);
        },
      };

      this.queue.push(task);

      timeoutId = setTimeout(() => {
        const wasActive = this.activeTask === task;
        this._removeTask(task);
        if (wasActive) {
          this.activeTask = null;
          this.isProcessing = false;
        }
        task.resolve({ success: false, message: 'Operation timed out' });
        if (wasActive) {
          this._processQueue();
        }
      }, PYODPS_COMMAND_TIMEOUT_MS);

      this._processQueue();
    });
  }

  close() {
    if (this.process) {
      this.process.kill();
      this.process = null;
    }
  }
}

// 全局进程管理器实例
const pythonProcessManager = new PythonProcessManager();

function runPyOdps(action, config) {
  return new Promise((resolve) => {
    const command = {
      action,
      endpoint: config.endpoint,
      project: config.projectName,
      access_id: config.accessId,
      access_key: config.accessKey,
      table_name: config.tableName || '',
      sql: config.sql || '',
      limit: config.limit,
      offset: config.offset || 0
    };

    pythonProcessManager.runCommand(command).then(resolve);
  });
}

// 退出时关闭 Python 进程
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
  }

  _extractRegionFromEndpoint(endpoint) {
    if (!endpoint) return 'cn-hangzhou';
    const match = endpoint.match(/service\.([a-z0-9-]+)\.maxcompute/);
    return match ? match[1].replace('-vpc', '').replace('-intranet', '') : 'cn-hangzhou';
  }

  async testConnection() {
    try {
      const result = await runPyOdps('test_connection', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey
      });

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
      } else {
        // 解析 MaxCompute 错误信息
        let errorMessage = result.message || '连接失败';
        
        // 常见错误类型解析
        if (errorMessage.includes('Invalid access key')) {
          errorMessage = 'Access Key 无效，请检查 AccessId 和 AccessKey 是否正确';
        } else if (errorMessage.includes('Project not found')) {
          errorMessage = '项目不存在，请检查项目名称是否正确';
        } else if (errorMessage.includes('Endpoint is not valid')) {
          errorMessage = 'Endpoint 无效，请检查服务地址是否正确';
        } else if (errorMessage.includes('Connection refused')) {
          errorMessage = '连接被拒绝，请检查网络连接和服务地址';
        } else if (errorMessage.includes('Timeout')) {
          errorMessage = '连接超时，请检查网络连接和服务状态';
        }
        
        return {
          success: false,
          message: errorMessage,
        };
      }
    } catch (error) {
      let errorMessage = error.message || '连接失败';
      
      // 解析 JavaScript 错误
      if (error.message.includes('Python 不可用')) {
        errorMessage = 'Python 环境不可用，请安装 Python 3.7+';
      } else if (error.message.includes('PyODPS 导入失败')) {
        errorMessage = 'PyODPS 库未安装，请运行: pip install pyodps';
      }
      
      return {
        success: false,
        message: errorMessage,
      };
    }
  }

  async getTables() {
    try {
      const result = await runPyOdps('get_tables', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey
      });

      if (result.success) {
        const tables = result.data || [];
        console.log(`[getTables] 返回${tables.length}个表`);
        return tables;
      } else {
        let errorMessage = result.message || '获取表列表失败';
        console.error('获取表列表失败:', errorMessage);
        throw new Error(errorMessage);
      }
    } catch (error) {
      let errorMessage = error.message || '获取表列表失败';
      console.error('获取表列表失败:', errorMessage);
      throw error;
    }
  }

  async getTableMeta(tableName) {
    try {
      console.log(`[getTableMeta] tableName=${tableName}`);
      
      const result = await runPyOdps('get_table_meta', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        tableName: tableName
      });

      if (result.success) {
        console.log(`[getTableMeta] 获取成功`);
        return result.data;
      } else {
        let errorMessage = result.message || '获取表元数据失败';
        if (errorMessage.includes('Table not found')) {
          errorMessage = `表 ${tableName} 不存在，请检查表名是否正确`;
        } else if (errorMessage.includes('Permission denied')) {
          errorMessage = `没有访问表 ${tableName} 的权限`;
        }
        throw new Error(errorMessage);
      }
    } catch (error) {
      console.error('获取表元数据失败:', error.message);
      throw error;
    }
  }

  async getTableData(tableName, limit = null, offset = 0) {
    try {
      console.log(`[getTableData] tableName=${tableName}, offset=${offset}, limit=${limit}`);

      const result = await runPyOdps('get_table_data', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        tableName: tableName,
        limit: limit,
        offset: offset
      });

      if (result.success) {
        const data = result.data || [];
        console.log(`[getTableData查询完成] 返回${data.length}条数据`);
        return data;
      } else {
        let errorMessage = result.message || '获取表数据失败';
        if (errorMessage.includes('Table not found')) {
          errorMessage = `表 ${tableName} 不存在，请检查表名是否正确`;
        } else if (errorMessage.includes('Permission denied')) {
          errorMessage = `没有访问表 ${tableName} 的权限`;
        } else if (errorMessage.includes('Query timeout')) {
          errorMessage = '查询超时，请尝试减少查询数据量或优化查询条件';
        }
        throw new Error(errorMessage);
      }
    } catch (error) {
      console.error('获取表数据失败:', error.message);
      throw error;
    }
  }

  async executeSQL(sql, limit = null, offset = 0) {
    try {
      let finalSQL = String(sql || '').trim().replace(/;+\s*$/, '');
      const hasLimitClause = /\blimit\s+\d+(\s+offset\s+\d+)?\s*$/i.test(finalSQL);
      if (limit !== null && !hasLimitClause) {
        finalSQL = `${finalSQL} LIMIT ${limit} OFFSET ${offset}`;
      }
      
      console.log(`[executeSQL] offset=${offset}, limit=${limit}, sql长度=${finalSQL.length}`);
      
      const result = await runPyOdps('execute_sql', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        sql: finalSQL,
        limit: limit,
        offset: offset
      });

      if (result.success) {
        const data = result.data || [];
        console.log(`[executeSQL查询完成] 返回${data.length}条数据`);
        return data;
      } else {
        // 解析错误信息
        let errorMessage = result.message || '执行 SQL 失败';
        if (errorMessage.includes('Syntax error')) {
          errorMessage = 'SQL 语法错误，请检查 SQL 语句';
        } else if (errorMessage.includes('Permission denied')) {
          errorMessage = '没有执行该 SQL 的权限';
        } else if (errorMessage.includes('Query timeout')) {
          errorMessage = '查询超时，请尝试优化 SQL 语句或减少查询数据量';
        } else if (errorMessage.includes('Table not found')) {
          errorMessage = 'SQL 中引用的表不存在，请检查表名';
        }
        throw new Error(errorMessage);
      }
    } catch (error) {
      console.error('执行 SQL 失败:', error.message);
      throw error;
    }
  }

  async close() {
    // PyODPS 不需要特殊关闭操作
  }
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
    throw new Error(`不支持的网络类型: ${networkType}，支持的网络类型: public, vpc, intranet`);
  }
  
  const endpoint = endpoints[region];
  if (!endpoint) {
    throw new Error(`不支持的区域: ${region}，请参考官方文档: https://help.aliyun.com/zh/maxcompute/user-guide/endpoints`);
  }
  
  return endpoint;
}

module.exports = { MaxComputeClient, getEndpoint, ENDPOINT_CONFIG };
