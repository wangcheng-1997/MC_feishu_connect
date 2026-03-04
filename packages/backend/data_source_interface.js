/**
 * 数据源接口
 * 定义所有数据源必须实现的方法
 */
class DataSourceInterface {
  /**
   * 测试连接
   * @returns {Promise<{success: boolean, message: string, data?: any}>}
   */
  async testConnection() {
    throw new Error('testConnection method must be implemented');
  }

  /**
   * 获取表列表
   * @returns {Promise<Array<{name: string, tableSchema?: string}>>}
   */
  async getTables() {
    throw new Error('getTables method must be implemented');
  }

  /**
   * 获取表元数据
   * @param {string} tableName - 表名
   * @returns {Promise<any>}
   */
  async getTableMeta(tableName) {
    throw new Error('getTableMeta method must be implemented');
  }

  /**
   * 获取表数据
   * @param {string} tableName - 表名
   * @param {number} limit - 限制数量
   * @param {number} offset - 偏移量
   * @returns {Promise<any>}
   */
  async getTableData(tableName, limit = 1000, offset = 0) {
    throw new Error('getTableData method must be implemented');
  }

  /**
   * 关闭连接
   * @returns {Promise<void>}
   */
  async close() {
    // 默认实现，子类可覆盖
  }
}

module.exports = DataSourceInterface;