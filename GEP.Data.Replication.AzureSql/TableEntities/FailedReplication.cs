using Microsoft.WindowsAzure.Storage.Table;

namespace GEP.Data.Replication.AzureSql
{
    public class FailedReplication : TableEntity
    {
        public string Guid { get; set; }
        public string RegionConfigKey { get; set; }
        public int FromRegionId { get; set; }
        public int ToRegionId { get; set; }
        public string Action { get; set; }
        public string TableName { get; set; }
        public string ErrorMessage { get; set; }
    }
}