using System;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using GEP.SMART.Storage.AzureSQL;
using Microsoft.Practices.EnterpriseLibrary.Common.Utility;
using GEP.SMART.Configuration;
using System.Linq;
using System.Threading.Tasks;
using GEP.Data.Replication.AzureSql.TableEntities;

namespace GEP.Data.Replication.AzureSql
{
    public static class SqlReplication
    {
        //DEV
        // Get from multiregion
        private const string _STORAGE_QUEUE = "replication-sqlevents";

        [FunctionName("processnewsqlevents")]
        public static async Task ProcessNewSqlEvents([QueueTrigger(_STORAGE_QUEUE, Connection = "AzureWebJobsStorage")]string queueItem, ILogger log)
        {
            try
            {
                await ManageSqlReplicationCMDB.MoveToProcessQueue(queueItem);
            }
            catch (Exception exp)
            {
                //Log to errorlog
                throw exp;
            }
        }

        [FunctionName("processTimeTrigger")]
        public static async Task ProcessTimeTrigger([TimerTrigger("0 */1 * * * *")]TimerInfo timerInfo)
        {
            try
            {
                await ProcessReplication(GetPrimaryRegion("RSPOC").RegionId);
            }
            catch (Exception exp)
            {
                //Log to errorlog
                throw exp;
            }
        }

		//[FunctionName("processFailedTriggers")]
  //      public static async Task ProcessFailedReplicationItems([TimerTrigger("0 */1 * * * *")]TimerInfo timerInfo)
  //      {
  //          try
  //          {
  //              //await ProcessFailedReplication();
  //          }
  //          catch (Exception exp)
  //          {
  //              //Log to errorlog
  //              throw exp;
  //          }
  //      }

        private static async Task ProcessFailedReplication()
        {
            List<FailedReplication> failedItems = await ManageSqlReplication.GetFailedItems();

        }

        private static async Task ProcessReplication(int RegionId)
        {
            //Get the itesm from the Cosmos table which are not from this region (Replicating other region updates)
            List<CommonProcessQueue> replicationMetaDatas = await ManageSqlReplicationCMDB.GetAvailableItems(RegionId);

            foreach (CommonProcessQueue replication in replicationMetaDatas)
            {
                // Need to check the Failed queue 

                //Stale Check in the Current Processing Queue
                bool isRecordExists = await ManageSqlReplicationCMDB.GetAvailableItemsBySqlGuid(replication, RegionId);

                bool isRecordExistsInfailed = await ManageSqlReplication.GetFailedItemsBySqlGuid(replication);

                if (!isRecordExists && !isRecordExistsInfailed)
                {
                    SqlCommand sqlCmd;
                    SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
                    DataSet secondaryDataSet = new DataSet();
                    FailedReplication failedReplication = null;
                    try
                    {
                        //ReplicationMetaData replication = JsonConvert.DeserializeObject<ReplicationMetaData>(data);
                        ReplicationRegion primaryRegion = GetPrimaryRegion(replication.ReplicationConfigKey);
                        List<ReplicationRegion> secondaryRegions = GetSecondaryActiveRegions(replication.ReplicationConfigKey); //Passing source regionID

                        DataTable updatedTable = GetPrimaryRegionDataForReplication(replication, primaryRegion);

                        var guids = updatedTable.AsEnumerable()
                            // .Where(r => r.Field<Guid>("Guid").Equals(new Guid(replication.Guid)))
                            .Select(r => r.Field<Guid>("Guid"))
                            .ToArray();
                        if (guids.Length > 0)
                        {
                            foreach (ReplicationRegion secondaryRegion in secondaryRegions)
                            {
                                using (SqlConnection sqlCon = new SqlConnection(secondaryRegion.ConnectionString))
                                {
                                    foreach (var primaryGuid in guids)
                                    {
                                        string filter = "Guid = '" + primaryGuid.ToString() + "'";
                                        DataRow[] updatedRow = updatedTable.Select(filter);
                                        sqlCmd = new SqlCommand("select * from " + replication.TableName + " where " + filter, sqlCon);
                                        sqlDataAdapter.SelectCommand = sqlCmd;
                                        SqlCommandBuilder cb = new SqlCommandBuilder(sqlDataAdapter);
                                        sqlDataAdapter.InsertCommand = cb.GetInsertCommand();
                                        sqlDataAdapter.UpdateCommand = cb.GetUpdateCommand();
                                        sqlDataAdapter.DeleteCommand = cb.GetDeleteCommand();
                                        sqlDataAdapter.Fill(secondaryDataSet, replication.TableName);

                                        int primaryRecord = updatedRow.Length;
                                        int secondaryRecord = secondaryDataSet.Tables[replication.TableName].Select(filter).Length;
                                        string operation = "";

                                        if (secondaryRecord > 0 && primaryRecord == 1) // UPDATE (or) SOFT DELETE
                                        {
                                            foreach (DataRow dr in updatedRow)
                                            {
                                                dr["isreplicated"] = true;
                                                secondaryDataSet.Tables[replication.TableName].Select(filter).SingleOrDefault().ItemArray = dr.ItemArray;
                                            }
                                            operation = "U";
                                        }
                                        if (secondaryRecord == 0 && primaryRecord == 1) // INSERT 
                                        {
                                            foreach (DataRow dr in updatedRow)
                                            {
                                                dr["isreplicated"] = true;
                                                secondaryDataSet.Tables[replication.TableName].Rows.Add(dr.ItemArray);
                                            }
                                            operation = "I";
                                        }
                                        //if (secondaryRecord == 1 && primaryRecord == 0) // HARD DELETE
                                        //{
                                        //   // secondaryDataSet.Tables[replication.TableName].Rows[0].Delete(); 
                                        //}
                                        try
                                        {
                                            failedReplication = new FailedReplication()
                                            {
                                                Action = operation,
                                                FromRegionId = primaryRegion.RegionId,
                                                ToRegionId = secondaryRegion.RegionId,
                                                TableName = replication.TableName,
                                                RegionConfigKey = replication.ReplicationConfigKey,
                                                Guid = primaryGuid.ToString()
                                            };
                                            ArchivedReplication archivedReplication = new ArchivedReplication()
                                            {
                                                Action = operation,
                                                FromRegionId = primaryRegion.RegionId,
                                                ToRegionId = secondaryRegion.RegionId,
                                                TableName = replication.TableName,
                                                RegionConfigKey = replication.ReplicationConfigKey,
                                                Guid = primaryGuid.ToString()
                                            };

                                            sqlDataAdapter.AcceptChangesDuringUpdate = true;
                                            int recordsAffected = sqlDataAdapter.Update(secondaryDataSet, replication.TableName);
                                            if (recordsAffected > 0)
                                            {
                                                UpdatePrimaryRegion(replication.ReplicationConfigKey, primaryGuid.ToString(), replication.TableName);
                                                await LogSqlReplication.LogArchive(archivedReplication);
                                                await ManageSqlReplicationCMDB.RemoveProcessQueueItem(replication);
                                            }
                                            //else
                                            //{
                                            //    failedReplication.ErrorMessage = "No matched record found (or) No records to update in destination from sources";
                                            //    await LogSqlReplication.LogFailed(failedReplication); // NOT REPLICATED & KEEP IN PROCESS ENABLE in PROCESS QUEUE
                                            //}
                                        }
                                        catch (Exception exp)
                                        {
                                            failedReplication.ErrorMessage = exp.StackTrace.ToString();
                                            await LogSqlReplication.LogFailed(failedReplication);
                                            await ManageSqlReplicationCMDB.RemoveProcessQueueItem(replication); // ERROR ON REPLICATION
                                        }
                                    }
                                }
                            }
                        }
                        else
                            await ManageSqlReplicationCMDB.RemoveProcessQueueItem(replication);

                    }
                    catch (Exception exp)
                    {
                        failedReplication.ErrorMessage = exp.StackTrace.ToString();
                        await LogSqlReplication.LogFailed(failedReplication);
                        await ManageSqlReplicationCMDB.RemoveProcessQueueItem(replication); // ERROR ON REPLICATION
                    }
                    finally
                    {
                        sqlDataAdapter.Dispose();
                        secondaryDataSet.Dispose();
                    }
                }
                else
                {
                    ConflictReplication conflictReplication = new ConflictReplication()
                    {
                        Guid = replication.Guid,
                        ReplicationSourceRegionId = replication.ReplicationSourceRegionId,
                        TransactionDateTime = replication.TransactionDateTime,
                        TableName = replication.TableName,
                        PartitionKey = replication.PartitionKey + "_CONFLICT",
                        RowKey = replication.RowKey,
                        Timestamp = replication.Timestamp
                    };
                    await LogSqlReplication.LogConflict(conflictReplication);
                    await ManageSqlReplicationCMDB.RemoveProcessQueueItem(replication); // Conflict Record
                }

            }
        }


        public static List<CommonProcessQueue> GetPrimaryRegionData(CommonProcessQueue sourceRegionMetadata, ReplicationRegion primaryRegion)
        {
            try
            {
                ReliableSqlDatabase sqlHelper = new ReliableSqlDatabase(primaryRegion.ConnectionString);
                SqlConnection objSqlCon = null;
                DataSet sourceDataSet = null;

                objSqlCon = (SqlConnection)sqlHelper.CreateConnection();
                objSqlCon.Open();
                using (SqlCommand objSqlCommand = new SqlCommand("select * from " + sourceRegionMetadata.TableName + " where isreplicated = 0"))
                {
                    objSqlCommand.CommandType = CommandType.Text;
                    sourceDataSet = sqlHelper.ExecuteDataSet(objSqlCommand);
                }
                sourceDataSet.Tables[0].TableName = sourceRegionMetadata.TableName;

                List<CommonProcessQueue> cpqList = new List<CommonProcessQueue>();

                DataTable dataTable = sourceDataSet.Tables[0].Clone();
                dataTable.Columns["updatedon"].DateTimeMode = DataSetDateTime.Utc;
                dataTable = sourceDataSet.Tables[0];
                foreach (DataRow dr in dataTable.Rows)
                {
                    CommonProcessQueue cpq = new CommonProcessQueue
                    {
                        ReplicationConfigKey = sourceRegionMetadata.ReplicationConfigKey,
                        ReplicationSourceRegionId = sourceRegionMetadata.ReplicationSourceRegionId,
                        TransactionDateTime = ((DateTime)dr["updatedon"]).ToUniversalTime(),
                        Guid = dr["guid"].ToString(),
                        TableName = sourceRegionMetadata.TableName,
                        ProcessStatus = 1
                    };
                    cpqList.Add(cpq);
                }

                return cpqList;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        public static DataTable GetPrimaryRegionDataForReplication(CommonProcessQueue sourceRegionMetadata, ReplicationRegion primaryRegion)
        {
            try
            {
                ReliableSqlDatabase sqlHelper = new ReliableSqlDatabase(primaryRegion.ConnectionString);
                SqlConnection objSqlCon = null;
                DataSet sourceDataSet = null;

                objSqlCon = (SqlConnection)sqlHelper.CreateConnection();
                objSqlCon.Open();
                using (SqlCommand objSqlCommand = new SqlCommand("select * from " + sourceRegionMetadata.TableName + " where isreplicated = 0"))
                {
                    objSqlCommand.CommandType = CommandType.Text;
                    sourceDataSet = sqlHelper.ExecuteDataSet(objSqlCommand);
                }
                sourceDataSet.Tables[0].TableName = sourceRegionMetadata.TableName;

                return sourceDataSet.Tables[0];
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        private static void UpdatePrimaryRegion(string productName, string uniqueId, string tableName)
        {
            Dictionary<string, string> primaryConfigs = MultiRegionConfig.GetConfigWithConfigKey(MultiRegionConfigTypes.PrimaryRegion, productName + "-RS-SQLCON");
            string connectionString = null;
            SqlConnection objSqlCon = null;
            primaryConfigs.ForEach(config => { connectionString = config.Value; });
            try
            {
                ReliableSqlDatabase sqlHelper = new ReliableSqlDatabase(connectionString);
                objSqlCon = (SqlConnection)sqlHelper.CreateConnection();
                objSqlCon.Open();
                SqlCommand objSqlCommand = new SqlCommand("UPDATE " + tableName + " SET isReplicated = 1 where Guid = '" + uniqueId + "'");
                objSqlCommand.CommandType = CommandType.Text;
                objSqlCommand.Connection = objSqlCon;
                objSqlCommand.ExecuteNonQuery();
            }
            catch (Exception exp)
            {
                throw exp;
            }
            finally
            {
                if (objSqlCon.State == ConnectionState.Open)
                    objSqlCon.Close();
            }
        }

        public static ReplicationRegion GetPrimaryRegion(string productName)
        {
            ReplicationRegion primaryRegion = null;
            Dictionary<string, string> primaryConfigs = MultiRegionConfig.GetConfigWithConfigKey(MultiRegionConfigTypes.PrimaryRegion, productName + "-RS-SQLCON");
            try
            {
                primaryConfigs.ForEach(config =>
                {
                    primaryRegion = new ReplicationRegion()
                    {
                        ConnectionString = config.Value,
                        isHealthy = true,
                        RegionId = Convert.ToInt32(config.Key.Split("~")[2]),
                        RegionKey = config.Key.Split("~")[1]
                    };
                });

                if (primaryRegion != null)
                    return primaryRegion;
                else
                    throw (new Exception("Primary connection is not avaiable"));
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        private static List<ReplicationRegion> GetSecondaryActiveRegions(string productName)
        {
            List<ReplicationRegion> activeSecondryRegions = new List<ReplicationRegion>();
            Dictionary<string, string> secondaryConfigs = MultiRegionConfig.GetConfigWithConfigKey(MultiRegionConfigTypes.SecondaryRegion, productName + "-RS-SQLCON");

            try
            {
                secondaryConfigs.ForEach(config =>
                {
                    activeSecondryRegions.Add(new ReplicationRegion()
                    {
                        ConnectionString = config.Value,
                        isHealthy = true,
                        RegionId = Convert.ToInt32(config.Key.Split("~")[2]),
                        RegionKey = config.Key.Split("~")[1]
                    });
                });

                var retunRegions = activeSecondryRegions.Distinct().ToList();

                if (retunRegions.Count == 0)
                    throw (new Exception("Atleast one Secondary Region is not avaiable"));
                else
                    return retunRegions;

            }
            catch (Exception exp)
            {
                throw exp;
            }
        }


    }
}
