const crypto = require('crypto');

// 请求签名秘钥，从环境变量读取
let secretKey = process.env.REQUEST_SIGN_SECRET || '';

// 设置秘钥
function setSecretKey(key) {
  secretKey = key;
}

// 获取秘钥
function getSecretKey() {
  return secretKey;
}

function judgeEncryptSignValid(req) {
  const headers = req.headers;
  const body = req.body;
  
  // 兼容不同大小写的 header 名称
  const nonce = headers["x-base-request-nonce"] || headers["X-Base-Request-Nonce"];
  const timestamp = headers["x-base-request-timestamp"] || headers["X-Base-Request-Timestamp"];
  const sig = headers["x-base-signature"] || headers["X-Base-Signature"];

  console.log("收到请求的所有 header keys:", Object.keys(headers));
  console.log("收到请求的header:");
  console.log("x-base-request-timestamp:", timestamp);
  console.log("  x-base-request-nonce:", nonce);
  console.log("  x-base-signature:", sig);
  console.log("  body:", body);
  console.log("  body type:", typeof body);

  // 当没有设置秘钥时，默认跳过验证，以提供向后兼容性
  if (!secretKey) {
    console.log("未设置请求签名秘钥，跳过签名验证");
    return true;
  }

  // 如果请求头中没有签名相关信息，说明是前端配置页面直接调用，跳过验证
  // 飞书平台调用会自动添加这些签名头
  if (!sig || !timestamp || !nonce) {
    console.log("请求头中缺少签名信息，可能是前端配置页面直接调用，跳过签名验证");
    return true;
  }
  
  // 处理 body 格式 - 飞书可能发送原始 JSON 字符串
  let bodyStr;
  if (typeof body === 'string') {
    bodyStr = body;
  } else if (body) {
    bodyStr = JSON.stringify(body);
  } else {
    bodyStr = '';
  }
  
  // 拼接字符串（按照官方文档顺序：timestamp + nonce + secretKey + body）
  const str = timestamp + nonce + secretKey + bodyStr;
  console.log("签名字符串:", str);
  
  // 创建SHA-1加密实例
  const sha1 = crypto.createHash("sha1");
  // 更新要加密的数据
  sha1.update(str, "utf8");
  // 计算加密结果
  const encryptedStr = sha1.digest("hex");
  // 比较加密结果
  const isValid = encryptedStr === sig;
  
  console.log(`签名验证: ${isValid ? '通过' : '失败'}, 计算结果: ${encryptedStr}, 收到签名: ${sig}`);
  
  return isValid;
}

module.exports = { judgeEncryptSignValid, setSecretKey, getSecretKey };
