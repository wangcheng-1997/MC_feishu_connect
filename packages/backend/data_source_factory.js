/**
 * 数据源工厂
 * 根据配置创建不同类型的数据源实例
 */
class DataSourceFactory {
  /**
   * 创建数据源实例
   * @param {Object} config - 数据源配置
   * @returns {Promise<DataSourceInterface>}
   */
  static async createDataSource(config) {
    if (config.maxcompute) {
      return this.createMaxComputeDataSource(config.maxcompute);
    } else if (config.sqlserver) {
      return this.createSqlServerDataSource(config.sqlserver);
    } else {
      throw new Error('Unsupported data source type');
    }
  }

  /**
   * 创建 MaxCompute 数据源
   * @param {Object} config - MaxCompute 配置
   * @returns {Promise<DataSourceInterface>}
   */
  static async createMaxComputeDataSource(config) {
    const { MaxComputeClient } = require('./maxcompute_client.js');
    
    console.log("原始配置:", JSON.stringify(config, null, 2));
    
    // 处理各种异常的数据格式
    let processedConfig = {
      accessId: '',
      accessKey: '',
      endpoint: '',
      projectName: '',
      schemaName: 'default'
    };
    
    // 提取扁平格式的配置
    if (typeof config === 'object') {
      // 处理嵌套格式
      if (config.accessId && typeof config.accessId === 'object') {
        console.log("检测到嵌套格式，尝试提取值");
        // 尝试从嵌套结构中提取
        processedConfig.accessId = this.extractValue(config, 'accessId');
        processedConfig.accessKey = this.extractValue(config, 'accessKey');
        processedConfig.endpoint = this.extractValue(config, 'endpoint');
        processedConfig.projectName = this.extractValue(config, 'projectName');
        processedConfig.schemaName = this.extractValue(config, 'schemaName') || 'default';
      } else {
        console.log("使用扁平格式");
        // 直接使用扁平格式
        processedConfig.accessId = config.accessId || '';
        processedConfig.accessKey = config.accessKey || '';
        processedConfig.endpoint = config.endpoint || '';
        processedConfig.projectName = config.projectName || '';
        processedConfig.schemaName = config.schemaName || 'default';
      }
    }
    
    console.log("处理后的配置:", JSON.stringify(processedConfig, null, 2));
    
    // 验证必要字段
    if (!processedConfig.accessId || !processedConfig.accessKey || !processedConfig.endpoint || !processedConfig.projectName) {
      console.error("缺少必要字段:", {
        accessId: !!processedConfig.accessId,
        accessKey: !!processedConfig.accessKey,
        endpoint: !!processedConfig.endpoint,
        projectName: !!processedConfig.projectName
      });
      throw new Error('缺少必要的 MaxCompute 配置参数');
    }
    
    return new MaxComputeClient(processedConfig);
  }
  
  // 辅助方法：从嵌套对象中提取值
  static extractValue(obj, key) {
    if (!obj) return '';
    
    // 直接获取
    if (obj[key] && typeof obj[key] !== 'object') {
      return obj[key];
    }
    
    // 递归查找
    for (let prop in obj) {
      if (obj[prop] && typeof obj[prop] === 'object') {
        const value = this.extractValue(obj[prop], key);
        if (value) return value;
      }
    }
    
    return '';
  }

  /**
   * 创建 SQL Server 数据源
   * @param {Object} config - SQL Server 配置
   * @returns {Promise<DataSourceInterface>}
   */
  static async createSqlServerDataSource(config) {
    const { SqlServerClient } = require('./sqlserver_client.js');
    return new SqlServerClient(config);
  }
}

module.exports = DataSourceFactory;