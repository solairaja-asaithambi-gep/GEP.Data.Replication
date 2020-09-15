using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace GEP.Data.Replication.AzureSql.Utility
{
    public static class Common
    {
        public static string GenerateRowKey()
        {
            var inverseTimeKey = DateTimeOffset
                            .MaxValue
                            .Subtract(DateTimeOffset.UtcNow)
                            .TotalMilliseconds
                            .ToString(CultureInfo.InvariantCulture);
            return string.Format("{0}-{1}", inverseTimeKey, Guid.NewGuid());
        }

        public static string GetEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name);
        }

        //public static DateTime GetUTCDatetime(DateTime dateTime)
        //{
        //    return DateTimeOffset.Parse(dateTime.ToString("o", CultureInfo.CreateSpecificCulture("en-US"))).UtcDateTime;
        //}
    }
}
