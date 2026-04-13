const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const cacheManager = require('./cache_manager.js');

const PYODPS_SCRIPT = `
import sys
import json
import argparse
import traceback

# 连接池缓存
connection_pool = {}

def get_connection(endpoint, project_name, access_id, access_key):
    try:
        import sys
        # 检查连接池
        key = f"{endpoint}:{project_name}:{access_id}"
        if key in connection_pool:
            print("从连接池获取连接", file=sys.stderr)
            return connection_pool[key]
        
        print("正在导入 PyODPS...", file=sys.stderr)
        from odps import ODPS
        print("PyODPS 导入成功", file=sys.stderr)
        
        odps = ODPS(
            access_id=access_id,
            secret_access_key=access_key,
            project=project_name,
            endpoint=endpoint
        )
        print("ODPS 实例创建成功", file=sys.stderr)
        
        # 缓存连接
        connection_pool[key] = odps
        return odps
    except ImportError as e:
        import sys
        print(f"PyODPS 导入失败: {str(e)}", file=sys.stderr)
        print("请安装 PyODPS: pip install pyodps", file=sys.stderr)
        raise
    except Exception as e:
        import sys
        print(f"创建连接失败: {str(e)}", file=sys.stderr)
        raise

def test_connection(endpoint, project_name, access_id, access_key):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        project = odps.get_project()
        return {'success': True, 'message': '连接成功', 'data': {'projectName': project.name}}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def get_tables(endpoint, project_name, access_id, access_key):
    try:
        import sys
        print(f"尝试连接到: {endpoint}, 项目: {project_name}", file=sys.stderr)
        odps = get_connection(endpoint, project_name, access_id, access_key)
        print("连接成功，正在获取表列表...", file=sys.stderr)
        tables = list(odps.list_tables())
        print(f"找到 {len(tables)} 个表", file=sys.stderr)
        return {'success': True, 'data': [{'name': t.name, 'schema': 'default'} for t in tables]}
    except Exception as e:
        import sys
        print(f"获取表列表失败: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return {'success': True, 'data': []}

def get_table_meta(endpoint, project_name, access_id, access_key, table_name):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        table = odps.get_table(table_name)
        
        columns = []
        for col in table.schema.columns:
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

def execute_sql(endpoint, project_name, access_id, access_key, sql, limit=1000):
    try:
        odps = get_connection(endpoint, project_name, access_id, access_key)
        
        instance = odps.execute_sql(sql)
        instance.wait_for_completion()
        
        with instance.open_reader() as reader:
            records = []
            row_count = 0
            schema = reader.schema
            for record in reader:
                if row_count >= limit:
                    break
                record_dict = {}
                for i, col in enumerate(schema.columns):
                    record_dict[col.name] = str(record[i]) if record[i] is not None else None
                records.append(record_dict)
                row_count += 1
        
        return {'success': True, 'data': records, 'total': row_count}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def get_table_data(endpoint, project_name, access_id, access_key, table_name, limit=1000, offset=0):
    sql = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
    return execute_sql(endpoint, project_name, access_id, access_key, sql, limit)

def handle_command(command):
    try:
        action = command.get('action')
        endpoint = command.get('endpoint')
        project = command.get('project')
        access_id = command.get('access_id')
        access_key = command.get('access_key')
        table_name = command.get('table_name', '')
        sql = command.get('sql', '')
        limit = command.get('limit', 1000)
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
        
        return result
    except Exception as e:
        import traceback
        traceback.print_exc(file=sys.stderr)
        return {'success': False, 'message': str(e)}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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
    
    if args.action == '--daemon':
        # 守护进程模式
        print('DAEMON_READY', flush=True)
        print('进入守护进程模式', file=sys.stderr)
        
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
                import traceback
                traceback.print_exc(file=sys.stderr)
                print(json.dumps({'success': False, 'message': str(e)}), flush=True)
    else:
        # 传统模式
        import sys
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
            
            # 只有 JSON 结果输出到 stdout，所有其他输出到 stderr
            print(json.dumps(result), file=sys.stdout)
            sys.stdout.flush()
        except Exception as e:
            print(json.dumps({'success': False, 'message': str(e)}), file=sys.stdout)
            sys.stdout.flush()
`;

const PYODPS_SCRIPT_PATH = path.join(__dirname, 'pyodps_runner.py');

if (!fs.existsSync(PYODPS_SCRIPT_PATH)) {
  fs.writeFileSync(PYODPS_SCRIPT_PATH, PYODPS_SCRIPT);
}

// Python 进程管理类
class PythonProcessManager {
  constructor() {
    this.process = null;
    this.queue = [];
    this.isProcessing = false;
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
    this._sendCommand(task.command);
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
    return new Promise(async (resolve, reject) => {
      // 创建任务
      const task = {
        command,
        resolve: (result) => {
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
      }, 30000);

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
      offset: config.offset || 0
    };

    console.log('执行 PyODPS:', action, config.tableName || config.sql || '');
    
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
      // 尝试从缓存获取
      const cacheKey = {
        dataSourceType: 'maxcompute',
        endpoint: this.endpoint,
        projectName: this.projectName
      };
      const cachedTables = cacheManager.getCachedTables(cacheKey);
      if (cachedTables) {
        return cachedTables;
      }

      const result = await runPyOdps('get_tables', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey
      });

      if (result.success) {
        const tables = result.data || [];
        // 缓存结果
        cacheManager.cacheTables(cacheKey, tables);
        return tables;
      } else {
        // 解析错误信息
        let errorMessage = result.message || '获取表列表失败';
        console.error('获取表列表失败:', errorMessage);
        return [];
      }
    } catch (error) {
      let errorMessage = error.message || '获取表列表失败';
      console.error('获取表列表失败:', errorMessage);
      return [];
    }
  }

  async getTableMeta(tableName) {
    try {
      // 尝试从缓存获取
      const cacheKey = {
        dataSourceType: 'maxcompute',
        endpoint: this.endpoint,
        projectName: this.projectName,
        tableName: tableName
      };
      const cachedMeta = cacheManager.getCachedTableMeta(cacheKey);
      if (cachedMeta) {
        return cachedMeta;
      }

      const result = await runPyOdps('get_table_meta', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        tableName: tableName
      });

      if (result.success) {
        // 缓存结果
        cacheManager.cacheTableMeta(cacheKey, result.data);
        return result.data;
      } else {
        // 解析错误信息
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

  async getTableData(tableName, limit = 1000, offset = 0) {
    try {
      // 尝试从缓存获取
      const cacheKey = {
        dataSourceType: 'maxcompute',
        endpoint: this.endpoint,
        projectName: this.projectName,
        tableName: tableName
      };
      const cachedData = cacheManager.get(cacheKey);
      if (cachedData) {
        return cachedData;
      }

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
        // 缓存结果
        cacheManager.set(cacheKey, data);
        return data;
      } else {
        // 解析错误信息
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

  async executeSQL(sql) {
    try {
      // 尝试从缓存获取
      const cacheKey = {
        dataSourceType: 'maxcompute',
        endpoint: this.endpoint,
        projectName: this.projectName,
        sql: sql
      };
      const cachedData = cacheManager.get(cacheKey);
      if (cachedData) {
        return cachedData;
      }

      const result = await runPyOdps('execute_sql', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        sql: sql
      });

      if (result.success) {
        const data = result.data || [];
        // 缓存结果
        cacheManager.set(cacheKey, data);
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
