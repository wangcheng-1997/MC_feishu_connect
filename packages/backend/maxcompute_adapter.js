/**
 * MaxCompute 数据类型与飞书多维表格字段类型映射适配器
 */

// MaxCompute 数据类型到飞书多维表格字段类型的映射
const ODPS_TO_LARK_TYPE_MAP = {
  // 字符串类型
  'STRING': 1,      // 文本
  'VARCHAR': 1,     // 文本
  'CHAR': 1,        // 文本
  
  // 数值类型
  'TINYINT': 2,     // 数字
  'SMALLINT': 2,    // 数字
  'INT': 2,         // 数字
  'INTEGER': 2,     // 数字
  'BIGINT': 2,      // 数字
  'FLOAT': 2,       // 数字
  'DOUBLE': 2,      // 数字
  'DECIMAL': 8,     // 货币
  
  // 日期时间类型
  'DATE': 5,        // 日期
  'DATETIME': 5,    // 日期
  'TIMESTAMP': 5,   // 日期
  
  // 布尔类型
  'BOOLEAN': 7,     // 复选框
};

// 字段类型名称映射（用于展示）
const FIELD_TYPE_NAMES = {
  1: '文本',
  2: '数字',
  3: '单选',
  4: '多选',
  5: '日期',
  6: '条形码',
  7: '复选框',
  8: '货币',
  9: '电话',
  10: '超链接',
  11: '进度',
  12: '评分',
  13: '人员',
  14: '附件',
  15: '关联',
  16: '公式',
  17: '地理位置',
  18: '群聊',
  19: '单向关联',
  20: '查找引用',
  21: '创建时间',
  22: '最后修改时间',
  23: '创建人',
  24: '最后修改人',
  25: '自动编号',
};

/**
 * 将 MaxCompute 字段类型转换为飞书多维表格字段类型
 */
function convertOdpsTypeToLark(odpsType) {
  const baseType = odpsType.toUpperCase().split('(')[0].trim();
  return ODPS_TO_LARK_TYPE_MAP[baseType] || 1; // 默认文本类型
}

/**
 * 获取字段属性配置
 */
function getFieldProperty(odpsType, odpsColumn) {
  const baseType = odpsType.toUpperCase().split('(')[0].trim();
  
  switch (baseType) {
    case 'DECIMAL':
      // 提取精度信息
      const match = odpsType.match(/\((\d+),\s*(\d+)\)/);
      const scale = match ? parseInt(match[2]) : 2;
      return {
        formatter: `#,##0.${'0'.repeat(scale)}`,
        currencyCode: 'CNY',
      };
    
    case 'DOUBLE':
    case 'FLOAT':
      return {
        formatter: '#,##0.00',
      };
    
    case 'BIGINT':
    case 'INT':
    case 'INTEGER':
      return {
        formatter: '#,##0',
      };
    
    case 'DATE':
    case 'DATETIME':
    case 'TIMESTAMP':
      return {
        formatter: 'yyyy/MM/dd HH:mm:ss',
      };
    
    default:
      return {};
  }
}

/**
 * 将 MaxCompute 列定义转换为飞书多维表格字段定义
 */
function convertColumnsToLarkFields(columns, primaryField = null) {
  return columns.map((col, index) => {
    const fieldType = convertOdpsTypeToLark(col.Type);
    const fieldId = `fid_${index + 1}`;
    
    return {
      fieldId: fieldId,
      fieldName: col.Name,
      fieldType: fieldType,
      isPrimary: primaryField ? col.Name === primaryField : index === 0,
      description: col.Comment || '',
      property: getFieldProperty(col.Type, col),
    };
  });
}

/**
 * 转换 MaxCompute 数据值为飞书多维表格格式
 */
function convertValueToLark(value, fieldType, odpsType) {
  if (value === null || value === undefined) {
    return null;
  }

  switch (fieldType) {
    case 1: // 文本
      return String(value);
    
    case 2: // 数字
    case 8: // 货币
      return Number(value);
    
    case 5: // 日期
      // MaxCompute 日期格式转换为时间戳（毫秒）
      if (typeof value === 'string') {
        return new Date(value).getTime();
      }
      return value;
    
    case 7: // 复选框
      return Boolean(value);
    
    case 10: // 超链接
      if (typeof value === 'string' && value.startsWith('http')) {
        return {
          name: value,
          url: value,
        };
      }
      return { name: String(value), url: String(value) };
    
    default:
      return value;
  }
}

/**
 * 将 MaxCompute 数据行转换为飞书多维表格记录格式
 */
function convertDataToLarkRecords(data, fields) {
  if (!Array.isArray(data) || data.length === 0) {
    return [];
  }

  return data.map((row, rowIndex) => {
    const record = {
      primaryId: `record_${rowIndex + 1}`,
      data: {},
    };

    fields.forEach(field => {
      const value = row[field.fieldName];
      record.data[field.fieldId] = convertValueToLark(value, field.fieldType, null);
    });

    return record;
  });
}

/**
 * 生成表元数据
 */
function generateTableMeta(tableName, columns, primaryField = null) {
  const fields = convertColumnsToLarkFields(columns, primaryField);
  
  return {
    tableName: tableName,
    fields: fields,
  };
}

/**
 * 生成表记录数据
 */
function generateTableRecords(data, fields, hasMore = false, nextPageToken = '') {
  const records = convertDataToLarkRecords(data, fields);
  
  return {
    nextPageToken: nextPageToken,
    hasMore: hasMore,
    records: records,
  };
}

module.exports = {
  convertOdpsTypeToLark,
  convertColumnsToLarkFields,
  convertDataToLarkRecords,
  convertValueToLark,
  generateTableMeta,
  generateTableRecords,
  FIELD_TYPE_NAMES,
  ODPS_TO_LARK_TYPE_MAP,
};
