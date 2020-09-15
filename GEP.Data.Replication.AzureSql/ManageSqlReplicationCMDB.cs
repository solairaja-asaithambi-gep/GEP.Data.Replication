using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;

namespace GEP.Data.Replication.AzureSql
{
    public static class ManageSqlReplicationCMDB
    {
        //private static string _COSMODB_REGION = "West US";
        private static string _TABLE_COMMON_PROCESS_QUEUE = "CommonProcessQueue";
        private static string _TABLE_STORAGE_ACCOUNT_CONNECTION = Utility.Common.GetEnvironmentVariable("AzureWebJobsStorage");
        private static string _COSMOS_TABLE_STORAGE_ACCOUNT_CONNECTION = Utility.Common.GetEnvironmentVariable("AzureCosmosDBStorage");
        private static CloudTable _CLOUDTABLE = GetCloudTable();
        private static CloudTable GetCloudTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_COSMOS_TABLE_STORAGE_ACCOUNT_CONNECTION);
            CloudTableClient _tableStorage = storageAccount.CreateCloudTableClient();
           // _tableStorage.TableClientConfiguration.CosmosExecutorConfiguration.CurrentRegion = _COSMODB_REGION; // DEV
           CloudTable _table = _tableStorage.GetTableReference(_TABLE_COMMON_PROCESS_QUEUE);
            return _table;
        }

        public static async Task MoveToProcessQueue(string data)
        {
            try
            {
                CommonProcessQueue queueItem = JsonConvert.DeserializeObject<CommonProcessQueue>(data);
                ReplicationRegion primaryRegion = SqlReplication.GetPrimaryRegion(queueItem.ReplicationConfigKey);
                List<CommonProcessQueue> updatedTable = SqlReplication.GetPrimaryRegionData(queueItem, primaryRegion);

                foreach (CommonProcessQueue item in updatedTable)
                {
                    bool inProgress = await CheckGuidProcessing(item, primaryRegion.RegionId);
                    if (!inProgress)
                    {
                        CommonProcessQueue cpq = new CommonProcessQueue()
                        {
                            PartitionKey = _TABLE_COMMON_PROCESS_QUEUE,
                            RowKey = item.Guid.ToString() + "_" + item.TransactionDateTime.ToString("MMddyyyhhmmssfffzz"),
                            Timestamp = DateTimeOffset.UtcNow,
                            ReplicationConfigKey = item.ReplicationConfigKey,
                            ReplicationSourceRegionId = item.ReplicationSourceRegionId,
                            TableName = queueItem.TableName,
                            TransactionDateTime = item.TransactionDateTime,
                            Guid = item.Guid.ToString(),
                            ProcessStatus = item.ProcessStatus,
                            ETag = "*"
                        };

                        TableResult insertTask = await _CLOUDTABLE.ExecuteAsync(TableOperation.InsertOrMerge(cpq));
                    }
                }
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static async Task<bool> GetAvailableItemsBySqlGuid(CommonProcessQueue cpq, int primaryRegionId)
        {
            try
            {
                List<CommonProcessQueue> newCpqItems = new List<CommonProcessQueue>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _CLOUDTABLE.ExecuteQuerySegmentedAsync(new TableQuery<CommonProcessQueue>(), token);
                    // CHECK For GUID which are arrived from different region later/delayed time are considered as conflict items.
                    newCpqItems.AddRange(queryResult.Results.Where(i => i.Guid == cpq.Guid
                    && i.ReplicationSourceRegionId != primaryRegionId
                  && i.ProcessStatus == 1 && i.TransactionDateTime < cpq.TransactionDateTime).ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);
                bool itemExists = newCpqItems.Count() > 0 ? true : false;
                return itemExists;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static async Task<List<CommonProcessQueue>> GetAvailableItems(int PrimaryRegionId)
        {
            try
            {
                List<CommonProcessQueue> newCpqItems = new List<CommonProcessQueue>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _CLOUDTABLE.ExecuteQuerySegmentedAsync(new TableQuery<CommonProcessQueue>(), token);
                    newCpqItems.AddRange(queryResult.Results.Where(i =>
                    i.ProcessStatus == 1
                    && i.ReplicationSourceRegionId == PrimaryRegionId
                    ).ToList());
                    //newCpqItems.AddRange(queryResult.Results.ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);
                return newCpqItems;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static async Task<bool> CheckGuidProcessing(CommonProcessQueue cpqItem, int primaryRegion)
        {
            try
            {
                List<CommonProcessQueue> newCpqItems = new List<CommonProcessQueue>();
                TableContinuationToken token = new TableContinuationToken();
                do
                {
                    var queryResult = await _CLOUDTABLE.ExecuteQuerySegmentedAsync(new TableQuery<CommonProcessQueue>(), token);
                    newCpqItems.AddRange(queryResult.Results.Where(i =>
                    {
                        return
                        i.ReplicationSourceRegionId != primaryRegion
                        && i.ReplicationSourceRegionId == cpqItem.ReplicationSourceRegionId
                        && i.ReplicationConfigKey == cpqItem.ReplicationConfigKey
                        && i.TableName == cpqItem.TableName
                        && DateTime.Compare(i.TransactionDateTime, cpqItem.TransactionDateTime) == 0
                        && i.Guid == cpqItem.Guid
                        && i.ProcessStatus == cpqItem.ProcessStatus;
                    }).ToList());
                    token = queryResult.ContinuationToken;
                } while (token != null);

                bool itemExists = newCpqItems.Count() > 0 ? true : false;
                return itemExists;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static async Task RemoveProcessQueueItem(CommonProcessQueue cpqItem)
        {
            try
            {
                TableOperation insertOrMergeOperation = null;
                
                TableOperation retrieveOperation = TableOperation.Retrieve<CommonProcessQueue>(cpqItem.PartitionKey, cpqItem.RowKey);
                TableResult result = await _CLOUDTABLE.ExecuteAsync(retrieveOperation);
                CommonProcessQueue cpqChkItem = result.Result as CommonProcessQueue;
                //cpqItem.ProcessStatus = 0;
                if (cpqChkItem != null)
                {
                    insertOrMergeOperation = TableOperation.Delete(cpqItem);
                    var updateTask = await _CLOUDTABLE.ExecuteAsync(insertOrMergeOperation);
                }
            }
            catch (Exception exp)
            {
                if (exp.Message == "The specified resource does not exist.")
                {
                    // SUPPRESS DUE TO REPLICATION IN AZURE COSMOS DELETE LATENCY
                }
                else if (exp.Message == "The update condition specified in the request was not satisfied.")
                {
                    // SUPPRESS DUE TO REPLICATION IN AZURE COSMOS DELETE LATENCY
                }
                else
                    throw exp;
            }
        }
    }
}
