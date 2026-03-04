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

  if (!sig) {
    console.log("无签名加密");
    return true;
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
  return encryptedStr === sig;
}

module.exports = { judgeEncryptSignValid };
