/**
 * 缓存管理器
 * 用于缓存查询结果，减少重复查询的开销
 */
class CacheManager {
  constructor() {
    this.cache = new Map();
    this.defaultTTL = 300000; // 默认缓存时间：5分钟
  }

  /**
   * 生成缓存键
   * @param {Object} config - 查询配置
   * @returns {string} 缓存键
   */
  generateKey(config) {
    if (!config) return null;
    
    // 提取关键参数
    const { dataSourceType, sql, tableName, limit, offset, endpoint, projectName, server, database, schema } = config;
    
    // 生成唯一键
    const keyParts = [
      dataSourceType,
      sql || tableName,
      limit || 1000,
      offset || 0,
      endpoint || server,
      projectName || database,
      schema || 'default'
    ];
    
    return keyParts.filter(Boolean).join('|');
  }

  /**
   * 设置缓存
   * @param {Object} config - 查询配置
   * @param {any} data - 要缓存的数据
   * @param {number} ttl - 缓存时间（毫秒）
   */
  set(config, data, ttl = this.defaultTTL) {
    const key = this.generateKey(config);
    if (!key) return;

    const item = {
      data,
      expireAt: Date.now() + ttl
    };

    this.cache.set(key, item);
    console.log(`缓存设置成功: ${key}, 过期时间: ${new Date(item.expireAt).toISOString()}`);
  }

  /**
   * 获取缓存
   * @param {Object} config - 查询配置
   * @returns {any|null} 缓存的数据或 null
   */
  get(config) {
    const key = this.generateKey(config);
    if (!key) return null;

    const item = this.cache.get(key);
    if (!item) {
      console.log(`缓存未命中: ${key}`);
      return null;
    }

    // 检查是否过期
    if (Date.now() > item.expireAt) {
      console.log(`缓存已过期: ${key}`);
      this.cache.delete(key);
      return null;
    }

    console.log(`缓存命中: ${key}`);
    return item.data;
  }

  /**
   * 删除缓存
   * @param {Object} config - 查询配置
   */
  delete(config) {
    const key = this.generateKey(config);
    if (!key) return;

    this.cache.delete(key);
    console.log(`缓存删除成功: ${key}`);
  }

  /**
   * 清除所有缓存
   */
  clear() {
    const size = this.cache.size;
    this.cache.clear();
    console.log(`缓存已全部清除，共 ${size} 个项目`);
  }

  /**
   * 获取缓存大小
   * @returns {number} 缓存项目数量
   */
  size() {
    return this.cache.size;
  }

  /**
   * 清理过期缓存
   */
  cleanup() {
    const now = Date.now();
    let deleted = 0;

    for (const [key, item] of this.cache.entries()) {
      if (now > item.expireAt) {
        this.cache.delete(key);
        deleted++;
      }
    }

    if (deleted > 0) {
      console.log(`清理了 ${deleted} 个过期缓存`);
    }
  }

  /**
   * 缓存表列表
   * @param {Object} config - 数据源配置
   * @param {Array} tables - 表列表
   */
  cacheTables(config, tables) {
    this.set({ ...config, action: 'get_tables' }, tables, 600000); // 表列表缓存10分钟
  }

  /**
   * 获取缓存的表列表
   * @param {Object} config - 数据源配置
   * @returns {Array|null} 表列表或 null
   */
  getCachedTables(config) {
    return this.get({ ...config, action: 'get_tables' });
  }

  /**
   * 缓存表元数据
   * @param {Object} config - 表配置
   * @param {Object} meta - 表元数据
   */
  cacheTableMeta(config, meta) {
    this.set({ ...config, action: 'get_table_meta' }, meta, 300000); // 元数据缓存5分钟
  }

  /**
   * 获取缓存的表元数据
   * @param {Object} config - 表配置
   * @returns {Object|null} 表元数据或 null
   */
  getCachedTableMeta(config) {
    return this.get({ ...config, action: 'get_table_meta' });
  }
}

// 导出单例实例
const cacheManager = new CacheManager();

// 定期清理过期缓存
setInterval(() => {
  cacheManager.cleanup();
}, 60000); // 每分钟清理一次

module.exports = cacheManager;
