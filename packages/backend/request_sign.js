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
  const nonce = headers["x-base-request-nonce"];
  const timestamp = headers["x-base-request-timestamp"];
  const sig = headers["x-base-signature"];

  console.log("收到请求的header:");
  console.log("x-base-request-timestamp:", headers["x-base-request-timestamp"]);
  console.log("  x-base-request-nonce:", headers["x-base-request-nonce"]);
  console.log("  x-base-signature:", headers["x-base-signature"]);

  // 如果没有设置秘钥，则跳过验证
  if (!secretKey) {
    console.log("未设置请求签名秘钥，跳过签名验证");
    return true;
  }

  if (!sig) {
    console.log("无签名，但已设置秘钥，验证失败");
    return false;
  }
  
  // 拼接字符串
  const str = timestamp + nonce + secretKey + JSON.stringify(body);
  // 创建SHA-1加密实例
  const sha1 = crypto.createHash("sha1");
  // 更新要加密的数据
  sha1.update(str);
  // 计算加密结果
  const encryptedStr = sha1.digest("hex");
  // 比较加密结果
  const isValid = encryptedStr === sig;
  
  console.log(`签名验证: ${isValid ? '通过' : '失败'}, 计算结果: ${encryptedStr}, 收到签名: ${sig}`);
  
  return isValid;
}

module.exports = { judgeEncryptSignValid, setSecretKey, getSecretKey };
