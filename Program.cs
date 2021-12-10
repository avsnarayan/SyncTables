using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using McMaster.Extensions.CommandLineUtils;

namespace SyncTables
{
    class Program
    {
        static void Main(string[] args)
        => CommandLineApplication.Execute<Program>(args);

        [Option(Description = "Source database connection string", ShortName ="sc")]
        public string SourceConnectionString { get; }

        [Option(Description = "Target database connection string", ShortName = "tc")]
        public string TargetConnectionString { get; }

        [Option(Description = "Table name", ShortName = "t")]
        public string TableName { get; }
        [Option(Description = "Key columns (CSV)", ShortName = "k")]
        public string KeyColumns { get; }

        [Option(ShortName = "n")]
        public int Count { get; }
        private void OnExecute()
        {
            Console.WriteLine($"SourceConnectionString : {SourceConnectionString}");
            Console.WriteLine($"TargetConnectionString : {TargetConnectionString}");
            Console.WriteLine($"TableName : {TableName}");
            if (ValidateConnections(SourceConnectionString, TargetConnectionString))
            {
                InsertRecords(SourceConnectionString, TargetConnectionString, TableName, KeyColumns);
            }
        }

        private void InsertRecords(string sourceConnectionString, string targetConnectionString, string tableName, string keyColumns)
        {
            var sourceKeyList = GetKeys(SourceConnectionString, TableName, KeyColumns);
            var targetKeyList = GetKeys(TargetConnectionString, TableName, KeyColumns);
            //var dt2 = DateTime.Now;
            var nonExistingRecords = sourceKeyList.Except(targetKeyList).ToList();
            //var dt3 = DateTime.Now;
            //Console.WriteLine($"Time :{(dt3 - dt2).Ticks}.");
            Console.WriteLine($"Inserting approximately {nonExistingRecords.Count} record(s).");


            using (var sourceConn = new SqlConnection(sourceConnectionString))
            using (var sourceAdapt = new SqlDataAdapter($"", new SqlConnection(sourceConnectionString)))
            using (var targetConn = new SqlConnection(targetConnectionString))
            using (var targetAdapt = new SqlDataAdapter($"SELECT * FROM {tableName} WHERE 1=0", new SqlConnection(targetConnectionString)))
            using (var targetCmdBuilder = new SqlCommandBuilder(targetAdapt))
            {
                var targetDs = new DataSet();
                
                targetAdapt.Fill(targetDs);
                Console.WriteLine($"Inserting..");
                var insertCommand = targetCmdBuilder.GetInsertCommand();
                targetAdapt.InsertCommand = insertCommand;
                var pageSize = 500;
                var fillSize = 0;
                foreach (var nonExistingRecord in nonExistingRecords)
                {
                    var selQuery = GetSelectQuery(tableName, GetColumnValueTuple(KeyColumns.Split(commaSeperator), nonExistingRecord));
                    sourceAdapt.SelectCommand.CommandText = selQuery;
                    var sourceDt = new DataTable();
                    sourceAdapt.Fill(sourceDt);
                    foreach (DataRow sourceRow in sourceDt.Rows)
                    {
                        var tempDr = targetDs.Tables[0].NewRow();
                        tempDr.ItemArray = sourceRow.ItemArray;
                        //tempDr.SetAdded();
                        targetDs.Tables[0].Rows.Add(tempDr);
                        fillSize++;
                    }

                    if (fillSize == pageSize)
                    {
                        targetAdapt.Update(targetDs);
                        targetDs.AcceptChanges();
                        fillSize = 0;
                    }
                }
                targetAdapt.Update(targetDs);
                targetDs.AcceptChanges();
                fillSize = 0;
            }
        }
        private char[] commaSeperator = new[] { ',' };
        private List<Tuple<string,string>> GetColumnValueTuple(string[] columns, string values)
        {
            var valueList = values.Split(commaSeperator);
            var colValTuples = new List<Tuple<string, string>>();
            for(var index = 0;index< columns.Length; index++)
            {
                colValTuples.Add(new Tuple<string,string>(columns[index],valueList[index]));
            }
            return colValTuples;
        }
        private string GetSelectQuery(string tableName,List<Tuple<string,string>> keyColumnValues)
        {
            var queryCondition = string.Empty;
            foreach(var keyColumnValue in keyColumnValues)
            {
                queryCondition += $"{keyColumnValue.Item1} = '{keyColumnValue.Item2}' AND ";
            }
            var lastParenSet = queryCondition.LastIndexOf("AND");

            queryCondition = queryCondition.Substring(0, lastParenSet > -1 ? lastParenSet : queryCondition.Count());
            var query = $"SELECT * FROM {tableName} WHERE {queryCondition}"; 
            return query;
        }
        private bool HasMultiKeyColumns(string keyColumns)
        {
            return keyColumns.Split(commaSeperator).Where(x => !string.IsNullOrWhiteSpace(x)).Count() > 1;
        }

        private List<string> GetKeys(string connString, string tableName, string keyColumns)
        {
            var query = string.Empty;
            
            if (HasMultiKeyColumns(keyColumns))
            {
                var concatStr = String.Join(",',',", keyColumns.Split(commaSeperator));
                query = $"SELECT CONCAT({concatStr}) AS _KEY_ FROM {tableName}";
            }
            else
            {
                query = $"SELECT  CONVERT(varchar(500),{keyColumns}) AS _KEY_ FROM {tableName}";
            }
                
            using (var connection = new SqlConnection(connString))
            using (var adapter = new SqlDataAdapter(query, connection))
            {
                var dataTable = new DataTable();
                
                adapter.Fill(dataTable);
                var keys = from x in dataTable.AsEnumerable() select x.Field<string>("_KEY_");
                return keys.ToList();
            }
        }

        public bool ValidateConnections(string sConnString, string tConnString)
        {
            var sourceConnection = new SqlConnection(sConnString);
            var targetConnection = new SqlConnection(tConnString);
            var isValid = false;
            try
            {
                sourceConnection.Open();
                isValid = true;
                Console.WriteLine("Sorce database connection validated.");
            }
            catch (Exception exp)
            {
                isValid = false;
                Console.WriteLine($"Invalid sorce database connection.{Environment.NewLine} Exception : {exp.Message}{Environment.NewLine}Stack trace: {exp.StackTrace}");
            }

            try
            {
                targetConnection.Open();
                isValid = true;
                Console.WriteLine("Target database connection validated.");
            }
            catch (Exception exp)
            {
                isValid = false;
                Console.WriteLine($"Invalid target database connection.{Environment.NewLine} Exception : {exp.Message}{Environment.NewLine}Stack trace: {exp.StackTrace}");
            }
            return isValid;
        }
    }
}
