using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SpotifyLoader
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
    public class BulkTableName : Attribute
    {
        public string Name { get; set; }
        public BulkTableName(string Name)
        {
            this.Name = Name;
        }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ColumnName : Attribute
    {
        public string Name { get; set; }
        public ColumnName(string Name)
        {
            this.Name = Name;
        }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ExcludeFromBulk : Attribute
    {

    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class AutoIncrementColumn : Attribute
    {

    }


    internal class InfluxSQL
    {
        private readonly string connectionString;
        public InfluxSQL(string connectionString)
        {
            this.connectionString = connectionString;
        }

        private void CreateDataTable<T>(out string tableName, out DataTable dt, out Dictionary<string, PropertyInfo> columnLookup)
        {
            tableName = typeof(T).GetCustomAttribute<BulkTableName>()?.Name ?? string.Empty;

            if (tableName == string.Empty)
            {
                tableName = typeof(T).Name;
            }

            columnLookup = new Dictionary<string, PropertyInfo>();

            dt = new DataTable(tableName);

            foreach (var item in typeof(T).GetProperties().Where(prop => prop.GetCustomAttribute<ExcludeFromBulk>() is null))
            {
                DataColumn column = new DataColumn();
                column.DataType = item.PropertyType;
                column.ColumnName = item.GetCustomAttribute<ColumnName>()?.Name ?? item.Name;

                bool isAutoIncrement = item.GetCustomAttribute<AutoIncrementColumn>() is not null;

                if (isAutoIncrement)
                {
                    column.AutoIncrement = true;
                }
                else
                {
                    columnLookup[column.ColumnName] = item;
                }

                dt.Columns.Add(column);
            }
        }

        public int BulkInsert<T>(IEnumerable<T> _data)
        {
            List<T> data = _data.ToList();

            if (data.Count == 0)
            {
                throw new Exception("Collection is empty");
            }

            CreateDataTable<T>(out string tableName, out DataTable dt, out Dictionary<string, PropertyInfo> columnLookup);

            var nameProp = data.First()?.GetType().GetProperty("Name");

            DataRow dr;
            foreach (var item in data)
            {
                dr = dt.NewRow();

                foreach (KeyValuePair<string, PropertyInfo> column in columnLookup)
                {
                    dr[column.Key] = column.Value.GetValue(item);
                }

                dt.Rows.Add(dr);
            }

            using (var bulk = new SqlBulkCopy(connectionString))
            {
                bulk.DestinationTableName = tableName;

                foreach (KeyValuePair<string, PropertyInfo> column in columnLookup)
                {
                    bulk.ColumnMappings.Add(column.Value.Name, column.Key);
                }

                bulk.WriteToServer(dt);

                return bulk.RowsCopied;
            }
        }
    }
}
