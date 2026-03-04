/**
 * 缓存管理器
 * 用于缓存表列表和元数据信息，提高性能
 */
class CacheManager {
  constructor() {
    this.cache = new Map();
    this.ttl = 3600000; // 缓存过期时间：1小时
  }

  /**
   * 生成缓存键
   * @param {string} prefix - 前缀
   * @param {Object} config - 配置对象
   * @returns {string}
   */
  generateKey(prefix, config) {
    const configStr = JSON.stringify(config);
    return `${prefix}:${Buffer.from(configStr).toString('base64')}`;
  }

  /**
   * 设置缓存
   * @param {string} key - 缓存键
   * @param {any} value - 缓存值
   */
  set(key, value) {
    const now = Date.now();
    this.cache.set(key, {
      value,
      timestamp: now,
      expiresAt: now + this.ttl
    });
  }

  /**
   * 获取缓存
   * @param {string} key - 缓存键
   * @returns {any|null}
   */
  get(key) {
    const item = this.cache.get(key);
    if (!item) return null;

    const now = Date.now();
    if (now > item.expiresAt) {
      this.cache.delete(key);
      return null;
    }

    return item.value;
  }

  /**
   * 删除缓存
   * @param {string} key - 缓存键
   */
  delete(key) {
    this.cache.delete(key);
  }

  /**
   * 清空缓存
   */
  clear() {
    this.cache.clear();
  }

  /**
   * 缓存表列表
   * @param {Object} config - 数据源配置
   * @param {Array} tables - 表列表
   */
  cacheTables(config, tables) {
    const key = this.generateKey('tables', config);
    this.set(key, tables);
  }

  /**
   * 获取缓存的表列表
   * @param {Object} config - 数据源配置
   * @returns {Array|null}
   */
  getCachedTables(config) {
    const key = this.generateKey('tables', config);
    return this.get(key);
  }

  /**
   * 缓存表元数据
   * @param {Object} config - 数据源配置
   * @param {string} tableName - 表名
   * @param {Object} meta - 表元数据
   */
  cacheTableMeta(config, tableName, meta) {
    const key = this.generateKey(`table_meta:${tableName}`, config);
    this.set(key, meta);
  }

  /**
   * 获取缓存的表元数据
   * @param {Object} config - 数据源配置
   * @param {string} tableName - 表名
   * @returns {Object|null}
   */
  getCachedTableMeta(config, tableName) {
    const key = this.generateKey(`table_meta:${tableName}`, config);
    return this.get(key);
  }
}

// 导出单例实例
module.exports = new CacheManager();