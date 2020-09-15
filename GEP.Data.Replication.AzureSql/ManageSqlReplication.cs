using GEP.SMART.Storage.AzureTable;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace GEP.Data.Replication.AzureSql
{
    public static class ManageSqlReplication
    {
        private static string _TABLE_FAILED_QUEUE = "FailedReplication";
        private static string _TABLE_ARCHIVE_QUEUE = "ArchivedReplication";
        private static string _TABLE_STORAGE_ACCOUNT_CONNECTION = Utility.Common.GetEnvironmentVariable("AzureWebJobsStorage");

        internal static async Task<bool> GetFailedItemsBySqlGuid(CommonProcessQueue replication)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            CloudTableClient _tableStorage = storageAccount.CreateCloudTableClient();
            CloudTable _table = _tableStorage.GetTableReference(_TABLE_FAILED_QUEUE);
            try
            {
                List<FailedReplication> failedItems = new List<FailedReplication>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _table.ExecuteQuerySegmentedAsync(new TableQuery<FailedReplication>(), token);
                    failedItems.AddRange(queryResult.Results.Where(i =>
                    i.Guid == replication.Guid
                    && replication.ReplicationSourceRegionId == i.FromRegionId
                    ).ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);

                if (failedItems.Count > 0)
                    return true;
                else
                    return false;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        //public static async Task MoveToProcessQueue(string data)
        //{
        //    ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
        //    CommonProcessQueue queueItem = JsonConvert.DeserializeObject<CommonProcessQueue>(data);

        //    try
        //    {
        //        var insertTask = await reliableTable.AddAsync(_TABLE_COMMON_PROCESS_QUEUE, new CommonProcessQueue()
        //        {
        //            PartitionKey = _TABLE_COMMON_PROCESS_QUEUE,
        //            RowKey = Utility.Common.GenerateRowKey(),
        //            Timestamp = DateTimeOffset.UtcNow,
        //            ReplicationConfigKey = queueItem.ReplicationConfigKey,
        //            ReplicationSourceRegionId = queueItem.ReplicationSourceRegionId,
        //            TableName = queueItem.TableName,
        //            TransactionDateTime = DateTimeOffset.Parse(queueItem.TransactionDateTime.ToString("o", CultureInfo.CreateSpecificCulture("en-US"))).UtcDateTime,
        //            Guid = queueItem.Guid,
        //            ProcessStatus = 1 // NEW
        //        });

        //        if (!insertTask)
        //            throw (new Exception("Error writing to " + _TABLE_COMMON_PROCESS_QUEUE));
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //    finally
        //    {
        //        reliableTable.Dispose();
        //    }
        //}

        //public static async Task<bool> GetAvailableItemsBySqlGuid(CommonProcessQueue cpq)
        //{
        //    ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
        //    CommonProcessQueue data = new CommonProcessQueue();
        //    try
        //    {
        //        IQueryable<CommonProcessQueue> processQueues = await reliableTable.GetAsync<CommonProcessQueue>(_TABLE_COMMON_PROCESS_QUEUE, cpq.PartitionKey, cpq.RowKey);
        //        bool itemExists = processQueues.Count() > 0 ? true : false;
        //        return itemExists;
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //    finally
        //    {
        //        reliableTable.Dispose();
        //    }
        //}

        public static async Task<List<FailedReplication>> GetFailedItems()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            CloudTableClient _tableStorage = storageAccount.CreateCloudTableClient();
            CloudTable _table = _tableStorage.GetTableReference(_TABLE_ARCHIVE_QUEUE);
            try
            {
                List<FailedReplication> failedItems = new List<FailedReplication>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _table.ExecuteQuerySegmentedAsync(new TableQuery<FailedReplication>(), token);
                    failedItems.AddRange(queryResult.Results.ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);
                return failedItems;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static async Task<bool> GetArchieveItemsByGuid(string Guid)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_TABLE_STORAGE_ACCOUNT_CONNECTION);
            CloudTableClient _tableStorage = storageAccount.CreateCloudTableClient();
            CloudTable _table = _tableStorage.GetTableReference(_TABLE_FAILED_QUEUE);
            try
            {
                List<ArchivedReplication> archivedReplications = new List<ArchivedReplication>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _table.ExecuteQuerySegmentedAsync(new TableQuery<ArchivedReplication>(), token);
                    archivedReplications.AddRange(queryResult.Results.Where(i =>
                    i.Guid == Guid).ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);

                if(archivedReplications.Count > 0)
                    return true;
                else
                    return false;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        //public static async Task UpdateProcessQueue(CommonProcessQueue cpqItem, int procStatus)
        //{
        //    ReliableTableStorage reliableTable = new ReliableTableStorage(_TABLE_STORAGE_ACCOUNT_CONNECTION);
        //    try
        //    {
        //        cpqItem.ProcessStatus = procStatus;
        //        var updateTask = await reliableTable.UpdateAsync(_TABLE_COMMON_PROCESS_QUEUE, cpqItem);

        //        if (!updateTask)
        //            throw (new Exception("Error writing to " + _TABLE_COMMON_PROCESS_QUEUE));
        //    }
        //    catch (Exception exp)
        //    {
        //        throw exp;
        //    }
        //    finally
        //    {
        //        reliableTable.Dispose();
        //    }
        //}


    }
}
