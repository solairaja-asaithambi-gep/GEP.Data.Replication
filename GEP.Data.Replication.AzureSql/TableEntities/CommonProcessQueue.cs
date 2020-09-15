﻿//using Microsoft.WindowsAzure.Storage.Table;

using Microsoft.Azure.Cosmos.Table;
using System;

namespace GEP.Data.Replication.AzureSql
{
    public class CommonProcessQueue : TableEntity
    {
        public int ReplicationSourceRegionId { get; set; }
        public string ReplicationConfigKey { get; set; }
        public DateTime TransactionDateTime { get; set; }
        public string TableName { get; set; }
        public string Guid{ get; set; }
        public int ProcessStatus { get; set; }
    }
}

