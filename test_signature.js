const crypto = require('crypto');

// 测试签名验证
function testSignature() {
  const secretKey = 'testBase';
  const timestamp = '1615470502';
  const nonce = '123456';
  const body = { test: 'data' };
  
  // 按照官方文档生成签名
  const str = timestamp + nonce + secretKey + JSON.stringify(body);
  const sha1 = crypto.createHash('sha1');
  sha1.update(str, 'utf8');
  const sig = sha1.digest('hex');
  
  console.log('测试签名生成:');
  console.log('Timestamp:', timestamp);
  console.log('Nonce:', nonce);
  console.log('SecretKey:', secretKey);
  console.log('Body:', body);
  console.log('签名字符串:', str);
  console.log('生成的签名:', sig);
  console.log('');
  
  return sig;
}

// 测试不同大小写的header
function testHeaderCase() {
  const testHeaders = [
    {
      'x-base-request-timestamp': '1615470502',
      'x-base-request-nonce': '123456',
      'x-base-signature': 'test'
    },
    {
      'X-Base-Request-Timestamp': '1615470502',
      'X-Base-Request-Nonce': '123456',
      'X-Base-Signature': 'test'
    }
  ];
  
  console.log('测试Header大小写兼容性:');
  testHeaders.forEach((headers, index) => {
    console.log(`测试 ${index + 1}:`);
    console.log('Headers:', headers);
    
    // 模拟judgeEncryptSignValid中的逻辑
    const nonce = headers["x-base-request-nonce"] || headers["X-Base-Request-Nonce"];
    const timestamp = headers["x-base-request-timestamp"] || headers["X-Base-Request-Timestamp"];
    const sig = headers["x-base-signature"] || headers["X-Base-Signature"];
    
    console.log('提取的nonce:', nonce);
    console.log('提取的timestamp:', timestamp);
    console.log('提取的sig:', sig);
    console.log('');
  });
}

// 运行测试
console.log('=== 签名验证测试 ===\n');
testSignature();
testHeaderCase();
console.log('=== 测试完成 ===');
