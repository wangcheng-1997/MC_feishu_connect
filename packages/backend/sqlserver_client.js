const sql = require('mssql');

/**
 * SQL Server 客户端
 * 支持连接 SQL Server 数据库并执行查询
 */
class SqlServerClient {
  constructor(config) {
    const port = parseInt(config.port, 10) || 1433;
    
    this.config = {
      server: config.server,
      port: port,
      database: config.database,
      user: config.user,
      password: config.password,
      options: {
        encrypt: config.encrypt !== false,
        trustServerCertificate: config.trustServerCertificate === true,
        connectTimeout: config.connectTimeout || 15000,
      },
      pool: {
        max: 20,
        min: 2,
        idleTimeoutMillis: 60000,
        acquireTimeoutMillis: 30000,
        createTimeoutMillis: 30000,
        destroyTimeoutMillis: 5000,
      },
      requestTimeout: config.requestTimeout || 60000,
    };
    this.pool = null;
  }

  async getPool() {
    if (!this.pool) {
      this.pool = await new sql.ConnectionPool(this.config).connect();
    }
    return this.pool;
  }

  async close() {
    if (this.pool) {
      await this.pool.close();
      this.pool = null;
    }
  }

  async executeQuery(query, params = {}, limit = null, offset = 0) {
    try {
      let finalQuery = query;
      if (limit !== null) {
        if (!query.toUpperCase().includes('ORDER BY')) {
          finalQuery = `SELECT * FROM (${query}) AS subquery ORDER BY (SELECT 1) OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
        } else {
          finalQuery = `${query} OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
        }
      }
      
      console.log(`[executeQuery] offset=${offset}, limit=${limit}, sql长度=${finalQuery.length}`);
      
      const pool = await this.getPool();
      const request = pool.request();

      if (Object.keys(params).length > 0) {
        Object.keys(params).forEach(key => {
          request.input(key, params[key]);
        });
      }

      const result = await request.query(finalQuery);
      const data = result.recordset || [];
      console.log(`[executeQuery完成] 返回${data.length}条数据`);
      return data;
    } catch (error) {
      console.error('SQL Server 查询执行失败:', error);
      throw error;
    }
  }

  async getTableMeta(tableName, schema = 'dbo') {
    try {
      const query = `
        SELECT 
          c.COLUMN_NAME AS name,
          c.DATA_TYPE AS type,
          c.CHARACTER_MAXIMUM_LENGTH AS maxLength,
          c.NUMERIC_PRECISION AS precision,
          c.NUMERIC_SCALE AS scale,
          c.IS_NULLABLE AS isNullable,
          c.COLUMN_DEFAULT AS defaultValue,
          ep.value AS description,
          CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS isPrimaryKey
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.extended_properties ep 
          ON ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
          AND ep.minor_id = c.ORDINAL_POSITION
          AND ep.name = 'MS_Description'
        LEFT JOIN (
          SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku 
            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
          WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
          AND c.TABLE_NAME = pk.TABLE_NAME 
          AND c.COLUMN_NAME = pk.COLUMN_NAME
        WHERE c.TABLE_NAME = @tableName 
          AND c.TABLE_SCHEMA = @schema
        ORDER BY c.ORDINAL_POSITION
      `;

      const columns = await this.executeQuery(query, { tableName, schema });
      
      const result = {
        tableName: tableName,
        schema: schema,
        columns: columns.map(col => ({
          Name: col.name,
          Type: this._formatDataType(col),
          MaxLength: col.maxLength,
          Precision: col.precision,
          Scale: col.scale,
          IsNullable: col.isNullable === 'YES',
          DefaultValue: col.defaultValue,
          Comment: col.description,
          IsPrimaryKey: col.isPrimaryKey === 1,
        })),
      };

      return result;
    } catch (error) {
      console.error('获取 SQL Server 表元数据失败:', error);
      throw error;
    }
  }

  _formatDataType(col) {
    let type = col.type.toUpperCase();
    
    if (col.maxLength && col.maxLength > 0) {
      if (type === 'VARCHAR' || type === 'NVARCHAR' || type === 'CHAR' || type === 'NCHAR') {
        type += `(${col.maxLength === -1 ? 'MAX' : col.maxLength})`;
      }
    }
    
    if (col.precision !== null && col.scale !== null) {
      if (type === 'DECIMAL' || type === 'NUMERIC') {
        type += `(${col.precision},${col.scale})`;
      }
    }
    
    return type;
  }

  async getTableData(tableName, schema = 'dbo', limit = 1000, offset = 0) {
    try {
      console.log(`[getTableData] tableName=${tableName}, schema=${schema}, offset=${offset}, limit=${limit}`);
      
      let orderByClause = 'ORDER BY (SELECT 1)';
      try {
        const meta = await this.getTableMeta(tableName, schema);
        const primaryKeyColumn = meta.columns.find(col => col.IsPrimaryKey);
        if (primaryKeyColumn) {
          orderByClause = `ORDER BY [${primaryKeyColumn.Name}]`;
        }
      } catch (e) {
        console.log('获取主键失败，使用默认排序:', e.message);
      }

      const query = `
        SELECT * FROM [${schema}].[${tableName}]
        ${orderByClause}
        OFFSET ${offset} ROWS
        FETCH NEXT ${limit} ROWS ONLY
      `;
      
      const data = await this.executeQuery(query);
      console.log(`[getTableData完成] 返回${data.length}条数据`);
      return data;
    } catch (error) {
      console.error('获取 SQL Server 表数据失败:', error);
      throw error;
    }
  }

  async getTables() {
    try {
      console.log('正在获取 SQL Server 表列表...');
      
      const query = `
        SELECT 
          TABLE_SCHEMA AS tableSchema,
          TABLE_NAME AS name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
      `;
      
      const tables = await this.executeQuery(query);
      console.log(`SQL Server 查询到 ${tables.length} 个表`);
      
      return tables.map(table => ({
        name: table.name,
        schema: table.tableSchema || 'dbo'
      }));
    } catch (error) {
      console.error('获取 SQL Server 表列表失败:', error);
      return [];
    }
  }

  async testConnection() {
    try {
      const pool = await this.getPool();
      await pool.request().query('SELECT 1 AS test');
      await this.close();
      return { success: true, message: '连接成功' };
    } catch (error) {
      await this.close();
      
      let errorMessage = error.message;
      
      if (error.code === 'ECONNRESET') {
        errorMessage = '连接被重置，可能原因：\n1. SQL Server 服务未启动\n2. 服务器地址或端口错误\n3. 防火墙阻止连接\n4. SQL Server 未启用 TCP/IP 协议';
      } else if (error.code === 'ECONNREFUSED') {
        errorMessage = '连接被拒绝，请检查：\n1. SQL Server 服务是否运行\n2. 服务器地址和端口是否正确\n3. SQL Server 是否允许远程连接';
      } else if (error.code === 'ETIMEDOUT' || error.code === 'ETIMEOUT') {
        errorMessage = '连接超时，请检查：\n1. 网络是否正常\n2. 服务器地址是否正确\n3. 防火墙设置';
      } else if (error.message && error.message.includes('Login failed')) {
        errorMessage = '登录失败，请检查用户名和密码是否正确';
      } else if (error.message && error.message.includes('Cannot open database')) {
        errorMessage = '无法打开数据库，请检查数据库名称是否正确';
      } else if (error.message && error.message.includes('self signed certificate')) {
        errorMessage = 'SSL 证书验证失败，请尝试开启"信任服务器证书"选项';
      }
      
      return { success: false, message: errorMessage };
    }
  }
}

module.exports = { SqlServerClient };
