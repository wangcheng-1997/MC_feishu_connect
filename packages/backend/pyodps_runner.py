
import sys
import json
import argparse

def get_connection(endpoint, project_name, access_id, access_key):
    from odps import ODPS
    
    odps = ODPS(
        access_id=access_id,
        secret_access_key=access_key,
        project=project_name,
        endpoint=endpoint
    )
    return odps

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
    
    print(json.dumps(result))
