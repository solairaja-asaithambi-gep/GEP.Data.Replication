using GEP.SMART.Storage.AzureTable;
using System;
using System.Globalization;
using System.Threading.Tasks;

namespace GEP.Data.Replication.AzureSql
{
    public static class LogSqlReplication
    {
        private static string _TABLE_ARCHIVED_REPLICATION = "ArchivedReplication";
        private static string _TABLE_FAILED_REPLICATION = "FailedReplication";
        private static string _TABLE_STORAGE_ACCOUNT = GetEnvironmentVariable("TableStorageAccount");
        private static string _TABLE_STORAGE_ACCOUNT_KEY = GetEnvironmentVariable("TableStorageAccountKey");

        public static async Task LogFailed(FailedReplication failedReplication)
        {
            ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT, _TABLE_STORAGE_ACCOUNT_KEY);
            try
            {
                bool trackStatus = await reliableTable.AddAsync(_TABLE_FAILED_REPLICATION, new FailedReplication()
                {
                    PartitionKey = _TABLE_FAILED_REPLICATION,
                    RowKey = GenerateRowKey(),
                    Timestamp = DateTimeOffset.UtcNow,
                    Guid = failedReplication.Guid,
                    TableName = failedReplication.TableName,
                    RegionConfigKey = failedReplication.RegionConfigKey,
                    FromRegionId = failedReplication.FromRegionId,
                    ToRegionId = failedReplication.ToRegionId

                });
                if (!trackStatus)
                    throw (new Exception("Error writing to " + _TABLE_FAILED_REPLICATION));
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static async Task LogArchive(ArchivedReplication archivedReplication)
        {
            ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT, _TABLE_STORAGE_ACCOUNT_KEY);
            try
            {
                var insertTask = await reliableTable.AddAsync(_TABLE_ARCHIVED_REPLICATION, new ArchivedReplication()
                {
                    PartitionKey = _TABLE_ARCHIVED_REPLICATION,
                    RowKey = GenerateRowKey(),
                    Timestamp = DateTimeOffset.UtcNow,
                    Guid = archivedReplication.Guid,
                    TableName = archivedReplication.TableName,
                    RegionConfigKey = archivedReplication.RegionConfigKey,
                    FromRegionId = archivedReplication.FromRegionId,
                    ToRegionId = archivedReplication.ToRegionId,
                    Action = archivedReplication.Action
                });

                if (!insertTask)
                    throw (new Exception("Error writing to " + _TABLE_ARCHIVED_REPLICATION));
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        private static string GenerateRowKey()
        {
            var inverseTimeKey = DateTimeOffset
                            .MaxValue
                            .Subtract(DateTimeOffset.UtcNow)
                            .TotalMilliseconds
                            .ToString(CultureInfo.InvariantCulture);
            return string.Format("{0}-{1}",inverseTimeKey, Guid.NewGuid());
        }

        public static string GetEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name);
        }
    }
}
