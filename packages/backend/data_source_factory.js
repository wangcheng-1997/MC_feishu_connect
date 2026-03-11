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
    
    // 处理嵌套的数据格式
    let processedConfig = config;
    if (config.accessId && typeof config.accessId === 'object') {
      // 提取嵌套的配置
      processedConfig = {
        accessId: config.accessId.accessId || config.accessId,
        accessKey: config.accessKey?.accessKey || config.accessKey,
        endpoint: config.endpoint?.endpoint || config.endpoint,
        projectName: config.projectName,
        schemaName: config.schemaName || 'default'
      };
    }
    
    return new MaxComputeClient(processedConfig);
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