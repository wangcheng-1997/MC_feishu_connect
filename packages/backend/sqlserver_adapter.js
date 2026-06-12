/**
 * SQL Server 数据类型与飞书多维表格字段类型映射适配器
 */

// SQL Server 数据类型到飞书多维表格字段类型的映射
const SQLSERVER_TO_LARK_TYPE_MAP = {
  // 字符串类型
  'VARCHAR': 1,       // 文本
  'NVARCHAR': 1,      // 文本
  'CHAR': 1,          // 文本
  'NCHAR': 1,         // 文本
  'TEXT': 1,          // 文本
  'NTEXT': 1,         // 文本
  
  // 数值类型 - 整数
  'TINYINT': 2,       // 数字
  'SMALLINT': 2,      // 数字
  'INT': 2,           // 数字
  'BIGINT': 2,        // 数字
  
  // 数值类型 - 浮点
  'REAL': 2,          // 数字
  'FLOAT': 2,         // 数字
  
  // 数值类型 - 精确小数
  'DECIMAL': 8,       // 货币
  'NUMERIC': 8,       // 货币
  'MONEY': 8,         // 货币
  'SMALLMONEY': 8,    // 货币
  
  // 日期时间类型
  'DATE': 5,          // 日期
  'TIME': 5,          // 日期
  'DATETIME': 5,      // 日期
  'DATETIME2': 5,     // 日期
  'SMALLDATETIME': 5, // 日期
  'DATETIMEOFFSET': 5,// 日期
  
  // 布尔类型
  'BIT': 7,           // 复选框
  
  // 二进制类型 - 转为文本存储
  'BINARY': 1,
  'VARBINARY': 1,
  'IMAGE': 1,
  
  // 其他类型
  'UNIQUEIDENTIFIER': 1, // GUID 转为文本
  'XML': 1,              // XML 转为文本
  'SQL_VARIANT': 1,      // 变体类型转为文本
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
 * 将 SQL Server 字段类型转换为飞书多维表格字段类型
 */
function convertSqlServerTypeToLark(sqlServerType) {
  const baseType = String(sqlServerType || '').toUpperCase().split('(')[0].trim();
  return SQLSERVER_TO_LARK_TYPE_MAP[baseType] || 1; // 默认文本类型
}

/**
 * 获取字段属性配置
 */
function getFieldProperty(sqlServerType, sqlServerColumn) {
  const typeText = String(sqlServerType || '');
  const baseType = typeText.toUpperCase().split('(')[0].trim();
  
  switch (baseType) {
    case 'DECIMAL':
    case 'NUMERIC':
      // 提取精度信息
      const match = typeText.match(/\((\d+),\s*(\d+)\)/);
      const scale = match ? parseInt(match[2]) : 2;
      return {
        formatter: `#,##0.${'0'.repeat(scale)}`,
        currencyCode: 'CNY',
      };
    
    case 'MONEY':
    case 'SMALLMONEY':
      return {
        formatter: '#,##0.00',
        currencyCode: 'CNY',
      };
    
    case 'REAL':
    case 'FLOAT':
      return {
        formatter: '#,##0.00',
      };
    
    case 'BIGINT':
    case 'INT':
    case 'SMALLINT':
    case 'TINYINT':
      return {
        formatter: '#,##0',
      };
    
    case 'DATE':
      return {
        formatter: 'yyyy/MM/dd',
      };
    
    case 'TIME':
      return {
        formatter: 'HH:mm',
      };
    
    case 'DATETIME':
    case 'DATETIME2':
    case 'SMALLDATETIME':
    case 'DATETIMEOFFSET':
      return {
        formatter: 'yyyy/MM/dd HH:mm',
      };
    
    default:
      return {};
  }
}

/**
 * 将 SQL Server 列定义转换为飞书多维表格字段定义
 */
function convertColumnsToLarkFields(columns, primaryField = null) {
  let primaryFieldIndex = -1;
  
  if (primaryField) {
    primaryFieldIndex = columns.findIndex(col => col.Name === primaryField);
  } else {
    primaryFieldIndex = columns.findIndex(col => col.IsPrimaryKey === true);
    if (primaryFieldIndex === -1) {
      primaryFieldIndex = 0;
    }
  }

  return columns.map((col, index) => {
    const fieldType = convertSqlServerTypeToLark(col.Type);
    const fieldId = `fid_${index + 1}`;
    
    const isPrimary = index === primaryFieldIndex;
    
    return {
      fieldId: fieldId,
      fieldName: col.Name,
      fieldType: fieldType,
      isPrimary: isPrimary,
      description: col.Comment || '',
      property: getFieldProperty(col.Type, col),
    };
  });
}

/**
 * 转换 SQL Server 数据值为飞书多维表格格式
 */
function convertValueToLark(value, fieldType, sqlServerType) {
  if (value === null || value === undefined) {
    return null;
  }

  switch (fieldType) {
    case 1: // 文本
      return String(value);
    
    case 2: // 数字
    case 8: // 货币
      {
        const numericValue = Number(value);
        return Number.isFinite(numericValue) ? numericValue : null;
      }
    
    case 5: // 日期
      // SQL Server 日期格式转换为时间戳（毫秒）
      if (value instanceof Date) {
        const timestamp = value.getTime();
        return Number.isFinite(timestamp) ? timestamp : null;
      }
      if (typeof value === 'string') {
        const timestamp = new Date(value).getTime();
        return Number.isFinite(timestamp) ? timestamp : null;
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
 * 将 SQL Server 数据行转换为飞书多维表格记录格式
 */
function getValueByFieldName(row, fieldName) {
  if (row[fieldName] !== undefined) {
    return row[fieldName];
  }
  const lowerFieldName = String(fieldName).toLowerCase();
  const matchedKey = Object.keys(row).find(key => key.toLowerCase() === lowerFieldName);
  return matchedKey ? row[matchedKey] : undefined;
}

function convertDataToLarkRecords(data, fields, offset = 0) {
  if (!Array.isArray(data) || data.length === 0) {
    return [];
  }

  return data.map((row, rowIndex) => {
    const record = {
      primaryId: `record_${offset + rowIndex + 1}`,
      data: {},
    };

    fields.forEach(field => {
      const value = getValueByFieldName(row, field.fieldName);
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
function generateTableRecords(data, fields, hasMore = false, nextPageToken = '', offset = 0) {
  const records = convertDataToLarkRecords(data, fields, offset);
  
  return {
    nextPageToken: nextPageToken,
    hasMore: hasMore,
    records: records,
  };
}

module.exports = {
  convertSqlServerTypeToLark,
  convertColumnsToLarkFields,
  convertDataToLarkRecords,
  convertValueToLark,
  generateTableMeta,
  generateTableRecords,
  FIELD_TYPE_NAMES,
  SQLSERVER_TO_LARK_TYPE_MAP,
};
