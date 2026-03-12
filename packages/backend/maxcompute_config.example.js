/**
 * MaxCompute 配置示例
 * 
 * 根据阿里云官方文档 https://help.aliyun.com/zh/maxcompute/user-guide/endpoints
 * 更新了 Endpoint 配置格式
 */

const { MaxComputeClient, getEndpoint } = require('./maxcompute_client.js');

// 方式一：直接指定 Endpoint（推荐用于生产环境）
const config1 = {
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  endpoint: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api',
  projectName: 'your-project-name',
  schemaName: 'default'
};

// 方式二：使用区域和网络类型自动生成 Endpoint（推荐用于开发环境）
const config2 = {
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  region: 'cn-hangzhou',
  networkType: 'public',
  projectName: 'your-project-name',
  schemaName: 'default'
};

// 方式三：使用 VPC Endpoint（适用于阿里云内部网络）
const config3 = {
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  region: 'cn-hangzhou',
  networkType: 'vpc',
  projectName: 'your-project-name',
  schemaName: 'default'
};

// 方式四：使用云产品互联 Endpoint（适用于 Quick BI 等阿里云产品）
const config4 = {
  accessId: 'your-access-id',
  accessKey: 'your-access-key',
  region: 'cn-hangzhou',
  networkType: 'intran',
  projectName: 'your-project-name',
  schemaName: 'default'
};

// 使用示例
async function example() {
  try {
    const client = new MaxComputeClient(config2);
    
    // 测试连接
    const result = await client.testConnection();
    console.log('连接测试结果:', result);
    
    if (result.success) {
      // 获取表列表
      const tables = await client.getTables();
      console.log('表列表:', tables);
      
      // 获取表元数据
      if (tables.length > 0) {
        const tableMeta = await client.getTableMeta(tables[0].name);
        console.log('表元数据:', tableMeta);
      }
    }
  } catch (error) {
    console.error('错误:', error.message);
  }
}

// 获取所有支持的 Endpoint
function listAllEndpoints() {
  const { ENDPOINT_CONFIG } = require('./maxcompute_client.js');
  
  console.log('公网 Endpoint:');
  Object.entries(ENDPOINT_CONFIG.public).forEach(([region, endpoint]) => {
    console.log(`  ${region}: ${endpoint}`);
  });
  
  console.log('\nVPC Endpoint:');
  Object.entries(ENDPOINT_CONFIG.vpc).forEach(([region, endpoint]) => {
    console.log(`  ${region}: ${endpoint}`);
  });
}

// 导出配置
module.exports = {
  config1,
  config2,
  config3,
  config4,
  example,
  listAllEndpoints
};
