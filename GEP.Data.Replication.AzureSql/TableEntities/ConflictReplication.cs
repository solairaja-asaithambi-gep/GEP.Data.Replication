

using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace GEP.Data.Replication.AzureSql.TableEntities
{
    public class ConflictReplication : TableEntity
    {
        public int ReplicationSourceRegionId { get; set; }
        public string ReplicationConfigKey { get; set; }
        public DateTime TransactionDateTime { get; set; }
        public string TableName { get; set; }
        public string Guid { get; set; }
    }
}
