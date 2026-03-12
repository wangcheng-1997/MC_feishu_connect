const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const PYODPS_SCRIPT = `
import sys
import json
import argparse
import traceback

def get_connection(endpoint, project_name, access_id, access_key):
    try:
        import sys
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['test_connection', 'get_tables', 'get_table_meta', 'execute_sql', 'get_table_data'])
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--access_id', required=True)
    parser.add_argument('--access_key', required=True)
    parser.add_argument('--table_name', default='')
    parser.add_argument('--sql', default='')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--offset', type=int, default=0)
    
    args = parser.parse_args()
    
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

function runPyOdps(action, config) {
  return new Promise((resolve, reject) => {
    const args = [
      PYODPS_SCRIPT_PATH,
      action,
      '--endpoint', config.endpoint,
      '--project', config.projectName,
      '--access_id', config.accessId,
      '--access_key', config.accessKey
    ];

    if (config.tableName) {
      args.push('--table_name', config.tableName);
    }
    if (config.sql) {
      args.push('--sql', config.sql);
    }
    if (config.limit) {
      args.push('--limit', String(config.limit));
    }
    if (config.offset) {
      args.push('--offset', String(config.offset));
    }

    console.log('执行 PyODPS:', action, config.tableName || config.sql || '');
    
    // 检测 Python 环境
    const python = process.env.REPLIT ? 'python3.11' : (process.platform === 'win32' ? 'python' : 'python3');
    console.log('使用 Python 命令:', python);
    
    // 检查 Python 是否可用
    const checkPython = spawn(python, ['--version'], { cwd: __dirname });
    checkPython.on('close', (code) => {
      if (code !== 0) {
        console.error('Python 不可用，尝试备选命令');
        // 尝试备选 Python 命令
        const pythonAlt = process.platform === 'win32' ? 'python' : 'python3';
        const proc = spawn(pythonAlt, args, { cwd: __dirname });
        handleProcessOutput(proc, resolve);
      } else {
        const proc = spawn(python, args, { cwd: __dirname });
        handleProcessOutput(proc, resolve);
      }
    });
  });
}

function handleProcessOutput(proc, resolve) {
  let stdout = '';
  let stderr = '';

  proc.stdout.on('data', (data) => {
    stdout += data.toString();
  });

  proc.stderr.on('data', (data) => {
    stderr += data.toString();
  });

  proc.on('close', (code) => {
    console.log('Python 进程退出码:', code);
    
    if (stderr) {
      console.error('Python 错误输出:', stderr);
    }
    
    if (stdout) {
      console.log('Python 输出:', stdout);
    }

    if (code !== 0) {
      console.error('PyODPS 执行失败:', stderr);
      resolve({ success: false, message: stderr || `进程退出码: ${code}` });
      return;
    }

    try {
      const result = JSON.parse(stdout);
      resolve(result);
    } catch (e) {
      console.error('JSON 解析错误:', stdout);
      resolve({ success: false, message: `无法解析输出: ${stdout}` });
    }
  });
}

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
        return {
          success: false,
          message: result.message || '连接失败',
        };
      }
    } catch (error) {
      return {
        success: false,
        message: error.message,
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
        return result.data || [];
      } else {
        console.error('获取表列表失败:', result.message);
        return [];
      }
    } catch (error) {
      console.error('获取表列表失败:', error.message);
      return [];
    }
  }

  async getTableMeta(tableName) {
    try {
      const result = await runPyOdps('get_table_meta', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        tableName: tableName
      });

      if (result.success) {
        return result.data;
      } else {
        throw new Error(result.message);
      }
    } catch (error) {
      console.error('获取表元数据失败:', error.message);
      throw error;
    }
  }

  async getTableData(tableName, limit = 1000, offset = 0) {
    try {
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
        return result.data || [];
      } else {
        throw new Error(result.message);
      }
    } catch (error) {
      console.error('获取表数据失败:', error.message);
      throw error;
    }
  }

  async executeSQL(sql, limit = 1000) {
    try {
      const result = await runPyOdps('execute_sql', {
        endpoint: this.endpoint,
        projectName: this.projectName,
        accessId: this.accessId,
        accessKey: this.accessKey,
        sql: sql,
        limit: limit
      });

      if (result.success) {
        return result.data || [];
      } else {
        throw new Error(result.message);
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
