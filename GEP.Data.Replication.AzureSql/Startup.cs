using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using GEP.SMART.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.Configuration;
using Microsoft.Extensions.Configuration;
using System;

[assembly: FunctionsStartup(typeof(GEP.Data.Replication.AzureSql.Startup))]
namespace GEP.Data.Replication.AzureSql
{
    [ExcludeFromCodeCoverage]
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            MultiRegionConfig.InitMultiRegionConfig();
            NewRelic.Api.Agent.NewRelic.SetTransactionName("GEP.Data.Replication.AzureSql", "Start UP!");
        }
    }
}