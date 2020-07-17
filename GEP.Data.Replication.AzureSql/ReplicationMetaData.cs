using System;
using System.Collections.Generic;
using System.Text;

namespace GEP.Data.Replication.AzureSql
{
    public class ReplicationMetaData
    {
        public int ReplicationSourceRegionId { get; set; }
        public string ReplicationConfigKey { get; set; }
        public DateTime TransactionDateTime { get; set; }
        public string TableName { get; set; }
        public string Guid { get; set; }
    }

    public class ReplicationRegion
    {
        public string RegionKey { get; set; }
        public int RegionId { get; set; }
        public string ConnectionString { get; set; }
        public bool isHealthy { get; set; }
    }
}
