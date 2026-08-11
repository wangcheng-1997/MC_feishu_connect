const test = require('node:test');
const assert = require('node:assert/strict');

const { MaxComputeClient } = require('../maxcompute_client_fixed.js');
const { getTableMetaFromMaxCompute } = require('../table_meta_fixed.js');
const { SqlServerClient } = require('../sqlserver_client.js');
const { getSqlServerTableMeta } = require('../sqlserver_handler.js');

test('MaxCompute custom SQL metadata errors do not fall back to table metadata', async () => {
  const originalGetQueryMeta = MaxComputeClient.prototype.getQueryMeta;
  const originalGetTableMeta = MaxComputeClient.prototype.getTableMeta;
  let tableMetaCalls = 0;

  MaxComputeClient.prototype.getQueryMeta = async () => {
    throw new Error('column b.net_actual_amt cannot be resolved');
  };
  MaxComputeClient.prototype.getTableMeta = async () => {
    tableMetaCalls += 1;
    return { Table: { Columns: [{ Name: 'wrong_field', Type: 'STRING' }] } };
  };

  try {
    await assert.rejects(
      getTableMetaFromMaxCompute({ tableName: 'dws_all_sales_daily', sql: 'SELECT bad_column FROM source' }),
      /column b\.net_actual_amt cannot be resolved/
    );
    assert.equal(tableMetaCalls, 0);
  } finally {
    MaxComputeClient.prototype.getQueryMeta = originalGetQueryMeta;
    MaxComputeClient.prototype.getTableMeta = originalGetTableMeta;
  }
});

test('SQL Server custom SQL metadata errors do not fall back to table metadata', async () => {
  const originalGetQueryMeta = SqlServerClient.prototype.getQueryMeta;
  const originalGetTableMeta = SqlServerClient.prototype.getTableMeta;
  const originalClose = SqlServerClient.prototype.close;
  let tableMetaCalls = 0;

  SqlServerClient.prototype.getQueryMeta = async () => {
    throw new Error('Invalid column name');
  };
  SqlServerClient.prototype.getTableMeta = async () => {
    tableMetaCalls += 1;
    return { columns: [{ Name: 'wrong_field', Type: 'NVARCHAR' }] };
  };
  SqlServerClient.prototype.close = async () => {};

  try {
    await assert.rejects(
      getSqlServerTableMeta({ tableName: 'sales', sql: 'SELECT bad_column FROM sales' }),
      /Invalid column name/
    );
    assert.equal(tableMetaCalls, 0);
  } finally {
    SqlServerClient.prototype.getQueryMeta = originalGetQueryMeta;
    SqlServerClient.prototype.getTableMeta = originalGetTableMeta;
    SqlServerClient.prototype.close = originalClose;
  }
});
