
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
