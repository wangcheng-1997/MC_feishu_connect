const crypto = require('crypto');
const axios = require('axios');

/**
 * MaxCompute REST API 客户端
 * 使用阿里云 MaxCompute REST API 直接通信，无需 Python
 */
class MaxComputeClient {
  constructor(config) {
    this.accessId = config.accessId;
    this.accessKey = config.accessKey;
    this.endpoint = config.endpoint ? config.endpoint.replace(/\/$/, '') : '';
    this.projectName = config.projectName;
    this.schemaName = config.schemaName || 'default';
    this.region = config.region || this._extractRegionFromEndpoint(this.endpoint);
  }

  _extractRegionFromEndpoint(endpoint) {
    if (!endpoint) return 'cn-hangzhou';
    const match = endpoint.match(/service\.([a-z0-9-]+)\.maxcompute/);
    return match ? match[1].replace('-vpc', '').replace('-intranet', '') : 'cn-hangzhou';
  }

  /**
   * 生成 MaxCompute REST API 签名
   * Authorization: ODPS {accessKeyId}:{Signature}
   * Signature = Base64(HMAC-SHA1(AccessKeySecret, StringToSign))
   */
  _buildAuthHeader(method, headers, resource) {
    const contentMd5 = headers['Content-MD5'] || '';
    const contentType = headers['Content-Type'] || '';
    const date = headers['Date'];

    // CanonicalizedODPSHeaders: x-odps-* headers sorted and joined
    const odpsHeaders = Object.entries(headers)
      .filter(([k]) => k.toLowerCase().startsWith('x-odps-'))
      .sort(([a], [b]) => a.toLowerCase().localeCompare(b.toLowerCase()))
      .map(([k, v]) => `${k.toLowerCase()}:${v.trim()}`)
      .join('\n');

    const stringToSign = [
      method.toUpperCase(),
      contentMd5,
      contentType,
      date,
      odpsHeaders ? odpsHeaders + '\n' + resource : resource,
    ].join('\n');

    const signature = crypto
      .createHmac('sha1', this.accessKey)
      .update(stringToSign, 'utf8')
      .digest('base64');

    return `ODPS ${this.accessId}:${signature}`;
  }

  /**
   * 发起 MaxCompute REST API 请求
   */
  async _request(method, path, options = {}) {
    const date = new Date().toUTCString();
    const url = `${this.endpoint}${path}`;
    const headers = {
      'Date': date,
      'x-odps-user-agent': 'MaxComputeNodeClient/1.0',
      ...options.headers,
    };

    if (options.data && typeof options.data === 'string') {
      headers['Content-Type'] = headers['Content-Type'] || 'application/xml';
      const md5 = crypto.createHash('md5').update(options.data, 'utf8').digest('base64');
      headers['Content-MD5'] = md5;
    }

    headers['Authorization'] = this._buildAuthHeader(method, headers, path.split('?')[0]);

    try {
      const response = await axios({
        method,
        url,
        headers,
        data: options.data,
        params: options.params,
        timeout: 30000,
      });
      return response.data;
    } catch (error) {
      const msg = error.response
        ? `HTTP ${error.response.status}: ${JSON.stringify(error.response.data)}`
        : error.message;
      throw new Error(msg);
    }
  }

  /**
   * 测试连接
   */
  async testConnection() {
    try {
      const path = `/projects/${this.projectName}`;
      await this._request('GET', path);
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
    } catch (error) {
      return {
        success: false,
        message: `连接失败: ${error.message}`,
      };
    }
  }

  /**
   * 获取表列表
   */
  async getTables() {
    try {
      const path = `/projects/${this.projectName}/tables`;
      const data = await this._request('GET', path, { params: { maxitems: 1000 } });
      const tables = data.Tables || data.tables || [];
      return tables.map(t => ({
        name: t.Name || t.name,
        schema: this.schemaName,
      }));
    } catch (error) {
      console.error('获取表列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取表元数据
   */
  async getTableMeta(tableName) {
    try {
      const path = `/projects/${this.projectName}/tables/${tableName}`;
      const data = await this._request('GET', path);
      const tableData = data.Table || data;
      const schema = tableData.Schema || tableData.schema || {};
      const columns = schema.Columns || schema.columns || [];
      return {
        Table: {
          Name: tableData.Name || tableData.name || tableName,
          Columns: columns.map(col => ({
            Name: col.Name || col.name,
            Type: col.Type || col.type,
            Comment: col.Comment || col.comment || '',
          })),
        },
      };
    } catch (error) {
      console.error('获取表元数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 执行 SQL 查询 — 通过 MaxCompute 实例 API
   */
  async executeSQL(sql, limit = 1000) {
    // 1. 创建 SQL 实例
    const createPath = `/projects/${this.projectName}/instances`;
    const xmlBody = `<?xml version="1.0" encoding="UTF-8"?>
<Instance>
  <Job>
    <Priority>9</Priority>
    <Tasks>
      <SQL>
        <Name>AnonymousJob</Name>
        <Config>
          <Property><Name>settings</Name><Value>{"odps.sql.allow.fullscan":"true"}</Value></Property>
        </Config>
        <Query><![CDATA[${sql}]]></Query>
      </SQL>
    </Tasks>
    <DAG/>
  </Job>
</Instance>`;

    const createResp = await this._request('POST', createPath, {
      data: xmlBody,
      headers: { 'Content-Type': 'application/xml' },
    });

    // 从 Location 或响应中获取实例 ID
    const instanceId = createResp.Instance
      ? createResp.Instance.Id || createResp.Instance.id
      : null;

    if (!instanceId) {
      throw new Error('创建 SQL 实例失败，未返回实例ID');
    }

    // 2. 等待实例完成
    await this._waitForInstance(instanceId);

    // 3. 获取结果
    return await this._getInstanceResult(instanceId, limit);
  }

  async _waitForInstance(instanceId, maxWaitMs = 60000) {
    const statusPath = `/projects/${this.projectName}/instances/${instanceId}`;
    const startTime = Date.now();
    while (Date.now() - startTime < maxWaitMs) {
      const data = await this._request('GET', statusPath);
      const status = (data.Instance && (data.Instance.Status || data.Instance.status)) || '';
      if (status === 'Terminated') return;
      if (status === 'Failed' || status === 'Cancelled') {
        throw new Error(`SQL 执行失败，状态: ${status}`);
      }
      await new Promise(r => setTimeout(r, 1500));
    }
    throw new Error('SQL 执行超时');
  }

  async _getInstanceResult(instanceId, limit = 1000) {
    const resultPath = `/projects/${this.projectName}/instances/${instanceId}/result`;
    const data = await this._request('GET', resultPath, {
      params: { limit },
    });

    // 结果为 CSV 格式
    if (typeof data === 'string') {
      return this._parseCsvResult(data);
    }
    return data.Records || data.records || [];
  }

  _parseCsvResult(csv) {
    const lines = csv.trim().split('\n').filter(Boolean);
    if (lines.length === 0) return [];
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).map(line => {
      const values = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
      const record = {};
      headers.forEach((h, i) => { record[h] = values[i] !== undefined ? values[i] : null; });
      return record;
    });
  }

  /**
   * 获取表数据
   */
  async getTableData(tableName, limit = 1000, offset = 0) {
    const schema = this.schemaName && this.schemaName !== 'default'
      ? `${this.schemaName}.${tableName}`
      : tableName;
    const sql = offset > 0
      ? `SELECT * FROM ${schema} LIMIT ${offset}, ${limit}`
      : `SELECT * FROM ${schema} LIMIT ${limit}`;
    return await this.executeSQL(sql, limit);
  }

  async close() {
    // REST API 不需要关闭
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
  if (!endpoints) throw new Error(`不支持的网络类型: ${networkType}`);
  const endpoint = endpoints[region];
  if (!endpoint) throw new Error(`不支持的区域: ${region}`);
  return endpoint;
}

module.exports = { MaxComputeClient, getEndpoint, ENDPOINT_CONFIG };
