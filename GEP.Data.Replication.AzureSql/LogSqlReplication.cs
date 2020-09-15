using System;
using GEP.SMART.Storage.AzureTable;
using System.Threading.Tasks;
using GEP.Data.Replication.AzureSql.TableEntities;

namespace GEP.Data.Replication.AzureSql
{
    public static class LogSqlReplication
    {
        private static string _TABLE_ARCHIVED_REPLICATION = "ArchivedReplication";
        private static string _TABLE_FAILED_REPLICATION = "FailedReplication";
        private static string _TABLE_STORAGE_ACCOUNT_CONNECTION = Utility.Common.GetEnvironmentVariable("AzureWebJobsStorage");
        private static string _TABLE_CONFLICT_REPLICATION = "StaleRecords";


        public static async Task LogConflict(ConflictReplication conflictReplication)
        {
            ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            try
            {
                bool trackStatus = await reliableTable.AddAsync(_TABLE_CONFLICT_REPLICATION, new ConflictReplication()
                {
                    PartitionKey = _TABLE_CONFLICT_REPLICATION,
                    RowKey = conflictReplication.Guid + "_" + conflictReplication.TransactionDateTime.ToString("MMddyyyhhmmssfffzz"),
                    Timestamp = DateTimeOffset.UtcNow,
                    Guid = conflictReplication.Guid,
                    TableName = conflictReplication.TableName,
                    ReplicationSourceRegionId = conflictReplication.ReplicationSourceRegionId,
                    ReplicationConfigKey = conflictReplication.ReplicationConfigKey,
                    TransactionDateTime = conflictReplication.TransactionDateTime

                });
                if (!trackStatus)
                    throw (new Exception("Error writing to " + _TABLE_CONFLICT_REPLICATION));
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static async Task LogFailed(FailedReplication failedReplication)
        {
            ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            try
            {
                bool trackStatus = await reliableTable.AddAsync(_TABLE_FAILED_REPLICATION, new FailedReplication()
                {
                    PartitionKey = _TABLE_FAILED_REPLICATION,
                    RowKey = Utility.Common.GenerateRowKey(),
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
            ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            try
            {
                var insertTask = await reliableTable.AddAsync(_TABLE_ARCHIVED_REPLICATION, new ArchivedReplication()
                {
                    PartitionKey = _TABLE_ARCHIVED_REPLICATION,
                    RowKey = Utility.Common.GenerateRowKey(),
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

    }
}
