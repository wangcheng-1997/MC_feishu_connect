
import sys
import json
import argparse
import datetime
import os
import time
import uuid

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
            columns = [
                {
                    'Name': col.name,
                    'Type': str(col.type),
                    'Comment': ''
                }
                for col in schema.columns
            ]
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
        
        return {'success': True, 'data': records, 'columns': columns, 'total': row_count}
    except Exception as e:
        return {'success': False, 'message': str(e)}

def build_page_token(task_id, offset):
    return f"{task_id}:{offset}"

def write_json_file(file_path, payload):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False)

def record_to_dict(record, columns):
    record_dict = {}
    for i, col in enumerate(columns):
        val = record[i]
        if val is None:
            record_dict[col.name] = None
        elif isinstance(val, (datetime.datetime, datetime.date)):
            record_dict[col.name] = val.strftime("%Y-%m-%d %H:%M:%S")
        else:
            record_dict[col.name] = str(val)
    return record_dict

def execute_sql_to_cache(endpoint, project_name, access_id, access_key, sql, cache_dir, query_signature, page_size=1000, task_id=''):
    try:
        started_at = time.time()
        odps = get_connection(endpoint, project_name, access_id, access_key)
        page_size = max(int(page_size or 1000), 1)
        task_id = task_id or uuid.uuid4().hex
        os.makedirs(cache_dir, exist_ok=True)

        execute_started_at = time.time()
        instance = odps.execute_sql(sql)
        execute_ms = int((time.time() - execute_started_at) * 1000)

        wait_started_at = time.time()
        instance.wait_for_completion()
        wait_ms = int((time.time() - wait_started_at) * 1000)

        read_started_at = time.time()
        total = 0
        page_offset = 0
        page_rows = []
        first_page_rows = []
        pages = {}
        columns = []

        def flush_page(rows, offset, has_more):
            page_file = f"{task_id}_{offset}.page"
            page_payload = {
                'taskId': task_id,
                'offset': offset,
                'rows': rows,
                'hasMore': has_more,
                'updatedAt': int(time.time() * 1000)
            }
            write_json_file(os.path.join(cache_dir, page_file), page_payload)
            pages[str(offset)] = {
                'file': page_file,
                'count': len(rows),
                'hasMore': has_more,
                'updatedAt': page_payload['updatedAt']
            }

        with instance.open_reader() as reader:
            schema = reader.schema
            columns = [
                {
                    'Name': col.name,
                    'Type': str(col.type),
                    'Comment': ''
                }
                for col in schema.columns
            ]
            for record in reader:
                page_rows.append(record_to_dict(record, schema.columns))
                total += 1
                if len(page_rows) == page_size:
                    if page_offset == 0:
                        first_page_rows = list(page_rows)
                    flush_page(page_rows, page_offset, True)
                    page_offset += page_size
                    page_rows = []

        if page_rows or total == 0:
            if page_offset == 0:
                first_page_rows = list(page_rows)
            flush_page(page_rows, page_offset, False)
        elif pages:
            last_offset = str(page_offset - page_size)
            last_page = pages[last_offset]
            last_page['hasMore'] = False
            last_path = os.path.join(cache_dir, last_page['file'])
            with open(last_path, 'r', encoding='utf-8') as f:
                last_payload = json.load(f)
            last_payload['hasMore'] = False
            last_payload['updatedAt'] = int(time.time() * 1000)
            write_json_file(last_path, last_payload)
            last_page['updatedAt'] = last_payload['updatedAt']

        now_ms = int(time.time() * 1000)
        metadata = {
            'taskId': task_id,
            'createdAt': now_ms,
            'expiresAt': now_ms + 10 * 60 * 1000,
            'querySignature': query_signature or '',
            'total': total,
            'pageSize': page_size,
            'rows': None,
            'columns': columns,
            'pages': pages,
        }
        write_json_file(os.path.join(cache_dir, f"{task_id}.json"), metadata)

        read_write_ms = int((time.time() - read_started_at) * 1000)
        return {
            'success': True,
            'data': first_page_rows,
            'columns': columns,
            'taskId': task_id,
            'total': total,
            'hasMore': total > page_size,
            'nextPageToken': build_page_token(task_id, page_size) if total > page_size else '',
            'timing': {
                'executeMs': execute_ms,
                'waitMs': wait_ms,
                'readWriteMs': read_write_ms,
                'totalMs': int((time.time() - started_at) * 1000)
            }
        }
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
        trace_id = command.get('trace_id', '')
        limit = command.get('limit')
        offset = command.get('offset', 0)
        cache_dir = command.get('cache_dir', '')
        query_signature = command.get('query_signature', '')
        page_size = command.get('page_size', 1000)
        task_id = command.get('task_id', '')
        
        if action == 'test_connection':
            result = test_connection(endpoint, project, access_id, access_key)
        elif action == 'get_tables':
            result = get_tables(endpoint, project, access_id, access_key)
        elif action == 'get_table_meta':
            result = get_table_meta(endpoint, project, access_id, access_key, table_name)
        elif action == 'execute_sql':
            result = execute_sql(endpoint, project, access_id, access_key, sql, limit)
        elif action == 'execute_sql_to_cache':
            result = execute_sql_to_cache(endpoint, project, access_id, access_key, sql, cache_dir, query_signature, page_size, task_id)
        elif action == 'get_table_data':
            result = get_table_data(endpoint, project, access_id, access_key, table_name, limit, offset)
        else:
            result = {'success': False, 'message': f'未知操作: {action}'}
        
        if request_id is not None:
            result['request_id'] = request_id
        if trace_id:
            result['trace_id'] = trace_id
        return result
    except Exception as e:
        result = {'success': False, 'message': str(e)}
        if request_id is not None:
            result['request_id'] = request_id
        if 'trace_id' in locals() and trace_id:
            result['trace_id'] = trace_id
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
    parser.add_argument('--cache_dir', default='')
    parser.add_argument('--query_signature', default='')
    parser.add_argument('--page_size', type=int, default=1000)
    parser.add_argument('--task_id', default='')
    
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
            elif args.action == 'execute_sql_to_cache':
                result = execute_sql_to_cache(args.endpoint, args.project, args.access_id, args.access_key, args.sql, args.cache_dir, args.query_signature, args.page_size, args.task_id)
            elif args.action == 'get_table_data':
                result = get_table_data(args.endpoint, args.project, args.access_id, args.access_key, args.table_name, args.limit, args.offset)
            else:
                result = {'success': False, 'message': f'未知操作: {args.action}'}
            
            print(json.dumps(result), file=sys.stdout)
            sys.stdout.flush()
        except Exception as e:
            print(json.dumps({'success': False, 'message': str(e)}), file=sys.stdout)
            sys.stdout.flush()
