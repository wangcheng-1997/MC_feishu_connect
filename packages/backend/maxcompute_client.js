const axios = require('axios');
const crypto = require('crypto');

/**
 * MaxCompute Endpoint 配置
 * 根据官方文档 https://help.aliyun.com/zh/maxcompute/user-guide/endpoints
 */
const ENDPOINT_CONFIG = {
  // 公网 Endpoint
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
  // VPC Endpoint
  vpc: {
    'cn-hangzhou': 'https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api',
    'cn-shanghai': 'https://service.cn-shanghai-vpc.maxcompute.aliyun-inc.com/api',
    'cn-beijing': 'https://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api',
    'cn-zhangjiakou': 'https://service.cn-zhangjiakou-vpc.maxcompute.aliyun-inc.com/api',
    'cn-wulanchabu': 'https://service.cn-wulanchabu-vpc.maxcompute.aliyun-inc.com/api',
    'cn-shenzhen': 'https://service.cn-shenzhen-vpc.maxcompute.aliyun-inc.com/api',
    'cn-chengdu': 'https://service.cn-chengdu-vpc.maxcompute.aliyun-inc.com/api',
    'cn-hongkong': 'https://service.cn-hongkong-vpc.maxcompute.aliyun-inc.com/api',
    'ap-southeast-1': 'https://service.ap-southeast-1-vpc.maxcompute.aliyun-inc.com/api',
    'ap-northeast-1': 'https://service.ap-northeast-1-vpc.maxcompute.aliyun-inc.com/api',
    'eu-central-1': 'https://service.eu-central-1-vpc.maxcompute.aliyun-inc.com/api',
    'us-west-1': 'https://service.us-west-1-vpc.maxcompute.aliyun-inc.com/api',
    'us-east-1': 'https://service.us-east-1-vpc.maxcompute.aliyun-inc.com/api',
  },
  // 云产品互联 Endpoint
  intranet: {
    'cn-hangzhou': 'https://service.cn-hangzhou-intranet.maxcompute.aliyun-inc.com/api',
    'cn-shanghai': 'https://service.cn-shanghai-intranet.maxcompute.aliyun-inc.com/api',
    'cn-beijing': 'https://service.cn-beijing-intranet.maxcompute.aliyun-inc.com/api',
    'cn-zhangjiakou': 'https://service.cn-zhangjiakou-intranet.maxcompute.aliyun-inc.com/api',
    'cn-wulanchabu': 'https://service.cn-wulanchabu-intranet.maxcompute.aliyun-inc.com/api',
    'cn-shenzhen': 'https://service.cn-shenzhen-intranet.maxcompute.aliyun-inc.com/api',
    'cn-chengdu': 'https://service.cn-chengdu-intranet.maxcompute.aliyun-inc.com/api',
    'cn-hongkong': 'https://service.cn-hongkong-intranet.maxcompute.aliyun-inc.com/api',
    'ap-southeast-1': 'https://service.ap-southeast-1-intranet.maxcompute.aliyun-inc.com/api',
    'ap-northeast-1': 'https://service.ap-northeast-1-intranet.maxcompute.aliyun-inc.com/api',
    'eu-central-1': 'https://service.eu-central-1-intranet.maxcompute.aliyun-inc.com/api',
    'us-west-1': 'https://service.us-west-1-intranet.maxcompute.aliyun-inc.com/api',
    'us-east-1': 'https://service.us-east-1-intranet.maxcompute.aliyun-inc.com/api',
  },
};

/**
 * 获取 Endpoint
 * @param {string} region - 区域代码，如 'cn-hangzhou'
 * @param {string} networkType - 网络类型: 'public' (公网), 'vpc' (VPC), 'intranet' (云产品互联)
 * @returns {string} Endpoint URL
 */
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

/**
 * MaxCompute 客户端
 * 支持通过 MaxCompute Tunnel 和 REST API 获取数据
 * 官方文档: https://help.aliyun.com/zh/maxcompute/
 */
class MaxComputeClient {
  constructor(config) {
    this.accessId = config.accessId;
    this.accessKey = config.accessKey;
    this.endpoint = config.endpoint; // 如: https://service.cn-hangzhou.maxcompute.aliyun.com/api (公网) 或 https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api (VPC)
    this.projectName = config.projectName;
    this.schemaName = config.schemaName || 'default';
    this.region = config.region || this._extractRegionFromEndpoint(config.endpoint);
  }

  _extractRegionFromEndpoint(endpoint) {
    const match = endpoint.match(/service\.([a-z0-9-]+)\.maxcompute/);
    return match ? match[1].replace('-vpc', '').replace('-intranet', '') : 'cn-hangzhou';
  }

  /**
   * 计算 MaxCompute 签名
   */
  _sign(method, path, date, contentType = '', contentMd5 = '') {
    const stringToSign = `${method}\n${contentMd5}\n${contentType}\n${date}\n${path}`;
    console.log("签名字符串:", JSON.stringify(stringToSign));
    console.log("签名字符串内容:", stringToSign);
    console.log("签名参数:", { method, contentMd5, contentType, date, path });
    const signature = crypto
      .createHmac('sha1', this.accessKey)
      .update(stringToSign, 'utf8')
      .digest('base64');
    console.log("计算出的签名:", signature);
    return `ODPS ${this.accessId}:${signature}`;
  }

  /**
   * 获取请求头
   */
  _getHeaders(method, path, body = '') {
    const date = new Date().toUTCString();
    
    let contentType = '';
    let contentMd5 = '';
    
    if (method === 'POST' && body) {
      contentType = 'application/json';
        contentMd5 = crypto.createHash('md5').update(body).digest('base64');
    }
    
    const headers = {
      'Date': date,
      'x-odps-project-name': this.projectName,
      'x-odps-schema-name': this.schemaName,
    };
    
    if (contentType) {
      headers['Content-Type'] = contentType;
    }
    
    if (contentMd5) {
      headers['Content-MD5'] = contentMd5;
    }
    
    headers['Authorization'] = this._sign(method, path, date, contentType, contentMd5);
    
    return headers;
  }

  /**
   * 执行 SQL 查询
   */
  async executeSQL(sql) {
    try {
      // MaxCompute SQL 任务提交
      const path = `/projects/${this.projectName}/instances`;
      const url = `${this.endpoint}${path}`;
      
      const task = {
        Name: 'SQLTask',
        Type: 'SQL',
        Query: sql,
      };

      const body = JSON.stringify({
        Task: task,
        Priority: 1,
      });

      const headers = this._getHeaders('POST', path, body);
      
      const response = await axios.post(url, body, { headers });
      const instanceId = response.data.InstanceId;

      // 等待任务完成并获取结果
      return await this._waitForInstance(instanceId);
    } catch (error) {
      console.error('MaxCompute SQL 执行失败:', error.message);
      throw error;
    }
  }

  /**
   * 等待实例完成
   */
  async _waitForInstance(instanceId, maxRetries = 60) {
    const path = `/projects/${this.projectName}/instances/${instanceId}`;
    const url = `${this.endpoint}${path}`;

    for (let i = 0; i < maxRetries; i++) {
      const headers = this._getHeaders('GET', path);
      const response = await axios.get(url, { headers });
      
      const status = response.data.Instance.Status;
      
      if (status === 'Terminated') {
        // 获取结果
        return await this._getInstanceResult(instanceId);
      } else if (status === 'Failed') {
        throw new Error(`任务执行失败: ${response.data.Instance.Message}`);
      }

      // 等待 2 秒后重试
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    throw new Error('等待任务完成超时');
  }

  /**
   * 获取实例结果
   */
  async _getInstanceResult(instanceId) {
    // 通过 Tunnel 获取结果
    const path = `/projects/${this.projectName}/instances/${instanceId}/results`;
    const url = `${this.endpoint}${path}`;
    
    const headers = this._getHeaders('GET', path);
    const response = await axios.get(url, { headers });
    
    return response.data;
  }

  /**
   * 获取表的元数据
   */
  async getTableMeta(tableName) {
    try {
      const path = `/projects/${this.projectName}/tables/${tableName}`;
      const url = `${this.endpoint}${path}`;
      
      const headers = this._getHeaders('GET', path);
      const response = await axios.get(url, { headers });
      
      return response.data;
    } catch (error) {
      console.error('获取表元数据失败:', error.message);
      throw error;
    }
  }

  /**
   * 获取表数据（通过 SQL）
   */
  async getTableData(tableName, limit = 1000, offset = 0) {
    const sql = `SELECT * FROM ${tableName} LIMIT ${limit} OFFSET ${offset}`;
    return await this.executeSQL(sql);
  }

  /**
   * 获取表字段列表
   */
  async getTableColumns(tableName) {
    const tableMeta = await this.getTableMeta(tableName);
    return tableMeta.Table.Columns || [];
  }

  /**
   * 获取表列表
   */
  async getTables() {
    try {
      // 通过 SQL 查询获取表列表
      const sql = `SHOW TABLES IN ${this.schemaName}`;
      const result = await this.executeSQL(sql);
      
      // 解析结果
      const tables = [];
      if (result && result.Rows) {
        result.Rows.forEach(row => {
          if (row && row.length > 0) {
            tables.push({
              name: row[0],
              schema: this.schemaName
            });
          }
        });
      }
      
      return tables;
    } catch (error) {
      console.error('获取表列表失败:', error.message);
      throw error;
    }
  }

  /**
   * 验证配置
   */
  _validateConfig() {
    if (!this.accessId) {
      throw new Error('AccessKey ID 不能为空');
    }
    if (!this.accessKey) {
      throw new Error('AccessKey Secret 不能为空');
    }
    if (!this.endpoint) {
      throw new Error('Endpoint 不能为空');
    }
    if (!this.projectName) {
      throw new Error('项目名称不能为空');
    }
    
    // 验证 endpoint 格式
    if (!this.endpoint.match(/^https?:\/\/service\.[a-z0-9-]+\.maxcompute/)) {
      throw new Error('Endpoint 格式不正确，应为: https://service.{region}.maxcompute.aliyun.com/api');
    }
  }

  /**
   * 测试连接
   * 尝试获取项目信息来验证连接
   */
  async testConnection() {
    try {
      console.log("开始测试 MaxCompute 连接");
      console.log("配置信息:", {
        accessId: this.accessId,
        endpoint: this.endpoint,
        projectName: this.projectName,
        schemaName: this.schemaName,
        region: this.region
      });
      
      // 验证配置
      this._validateConfig();
      
      // 尝试获取项目信息来验证连接
      const path = `/projects/${this.projectName}`;
      const url = `${this.endpoint}${path}`;
      
      console.log("请求 URL:", url);
      console.log("请求路径:", path);
      
      const headers = this._getHeaders('GET', path);
      console.log("请求头:", JSON.stringify(headers, null, 2));
      
      const response = await axios.get(url, { headers, timeout: 10000 });
      
      console.log("响应状态:", response.status);
      console.log("响应数据:", JSON.stringify(response.data, null, 2));
      
      if (response.status === 200) {
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
      
      return {
        success: false,
        message: `连接失败，状态码: ${response.status}`,
      };
    } catch (error) {
      console.error('MaxCompute 连接测试失败:', error.message);
      console.error('错误详情:', error);
      
      if (error.response) {
        console.error('响应状态:', error.response.response?.status);
        console.error('响应数据:', JSON.stringify(error.response.response?.data, null, 2));
        console.error('响应头:', JSON.stringify(error.response.response?.headers, null, 2));
      }
      
      let errorMessage = '连接失败';
      if (error.response) {
        // 服务器返回了错误响应
        switch (error.response.status) {
          case 401:
            errorMessage = '认证失败，请检查 AccessKey ID 和 AccessKey Secret';
            break;
          case 403:
            errorMessage = '权限不足，请检查账号权限';
            break;
          case 404:
            errorMessage = '项目不存在，请检查项目名称';
            break;
          case 500:
            errorMessage = '服务器内部错误';
            break;
          default:
            errorMessage = `连接失败: ${error.response.status} - ${error.response.statusText}`;
        }
      } else if (error.code === 'ECONNREFUSED') {
        errorMessage = '无法连接到服务器，请检查网络或端点地址';
      } else if (error.code === 'ETIMEDOUT' || error.code === 'ECONNABORTED') {
        errorMessage = '连接超时，请检查网络或端点地址';
      } else {
        errorMessage = `连接失败: ${error.message}`;
      }
      
      return {
        success: false,
        message: errorMessage,
      };
    }
  }

  /**
   * 关闭连接
   */
  async close() {
    // MaxCompute 客户端不需要特殊的关闭操作
    // 因为它使用的是 HTTP 请求，没有长连接
  }
}

module.exports = { MaxComputeClient, getEndpoint, ENDPOINT_CONFIG };
