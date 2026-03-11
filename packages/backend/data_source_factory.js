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
    
    // 直接使用扁平格式配置
    const processedConfig = {
      accessId: config.accessId || '',
      accessKey: config.accessKey || '',
      endpoint: config.endpoint || '',
      projectName: config.projectName || '',
      schemaName: config.schemaName || 'default'
    };
    
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