using System;
using System.Data;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using GEP.SMART.Storage.AzureSQL;
using Microsoft.Practices.EnterpriseLibrary.Common.Utility;
using GEP.SMART.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace GEP.Data.Replication.AzureSql
{
    public static class SqlReplication
    {
        // Get from multiregion
        private const string _STORAGE_QUEUE = "replication-sqlevents";

        [FunctionName("processnewsqlevents")]
        public static async Task ProcessNewSqlEvents([QueueTrigger(_STORAGE_QUEUE, Connection = "AzureWebJobsStorage")]string queueItem, ILogger log)
        {
            try
            {
                await ProcessReplication(queueItem);
                //Log to transaction log table.
            }
            catch (Exception exp)
            {
                //Log to errorlog
                throw exp;
            }
        }

        private static async Task ProcessReplication(string data)
        {
            ReplicationMetaData replication = JsonConvert.DeserializeObject<ReplicationMetaData>(data);
            ReplicationRegion primaryRegion = GetPrimaryRegion(replication.ReplicationConfigKey);
            List<ReplicationRegion> secondaryRegions = GetSecondaryActiveRegions(replication.ReplicationConfigKey); //Passing source regionID

            DataTable updatedTable = GetPrimaryRegionData(replication, primaryRegion);

            var guids = updatedTable.AsEnumerable()
                .Select(r => r.Field<Guid>("Guid"))
                .ToArray();

            SqlCommand sqlCmd;
            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
            DataSet secondaryDataSet = new DataSet();

            try
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
                            sqlDataAdapter.AcceptChangesDuringUpdate = true;
                            int updates = sqlDataAdapter.Update(secondaryDataSet, replication.TableName);
                            if (updates == 0)
                            {
                                //
                            }
                            else
                            {
                                ArchivedReplication archivedReplication = new ArchivedReplication()
                                {
                                    Action = operation,
                                    FromRegionId = primaryRegion.RegionId,
                                    ToRegionId = secondaryRegion.RegionId,
                                    TableName = replication.TableName,
                                    RegionConfigKey = replication.ReplicationConfigKey,
                                    Guid = primaryGuid.ToString()
                                };
                                UpdatePrimaryRegion(replication.ReplicationConfigKey, primaryGuid.ToString(), replication.TableName);
                                await LogSqlReplication.LogArchive(archivedReplication);
                            }
                        }
                    }
                }
            }
            catch (Exception exp)
            {
                throw exp;
            }
            finally
            {
                sqlDataAdapter.Dispose();
                secondaryDataSet.Dispose();
            }
        }


        public static DataTable GetPrimaryRegionData(ReplicationMetaData sourceRegionMetadata, ReplicationRegion primaryRegion)
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

        private static ReplicationRegion GetPrimaryRegion(string productName)
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
                if (activeSecondryRegions.Count == 0)
                    throw (new Exception("Atleast one Secondary Region is not avaiable"));
                else
                    return activeSecondryRegions;

            }
            catch (Exception exp)
            {
                throw exp;
            }
        }


    }
}
