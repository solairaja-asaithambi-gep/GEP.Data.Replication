namespace GEP.Data.Replication.AzureSql
{
    public class ReplicationRegion
    {
        public string RegionKey { get; set; }
        public int RegionId { get; set; }
        public string ConnectionString { get; set; }
        public bool isHealthy { get; set; }
    }
}
