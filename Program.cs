using System;
using System.IO;
using System.Data.SqlClient;
using System.Timers;


namespace file_watcher_tool
{
    class Program
    {
        private static readonly string connectionString = @"Data Source = (localdb)\MSSQLLocalDB ; Initial Catalog= FileWatcherDB; Integrated Security = True;";

        private static readonly string lookupTableName = "LookUpTable";

        private static readonly string transactionalTableName = "TransactionalTable";

          static void Main(string[] args)
        {
          

            StartFileMonitoring();

            StartHourlyReportGenerator();

            Console.WriteLine("File monitoring started. Press anykey to exit.");
            Console.ReadKey();
        }

        static void StartFileMonitoring()
        {
            string parentDirectory = @"C:\sample_file_watcher";
            FileSystemWatcher watcher = new FileSystemWatcher
            {
                Path = parentDirectory,
                Filter = "*.*",
                IncludeSubdirectories = true,
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.LastAccess,
                
            };
            watcher.Created += OnFileCreated;
            watcher.Changed += OnFileChanged;
            watcher.Deleted += OnFileDeleted;
            watcher.Renamed += OnFileRenamed;
            watcher.EnableRaisingEvents = true;
        }
        static void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"A new file {e.Name} was created at {e.FullPath}");

            string FileName = e.Name;
            string FilePath = e.FullPath;
            DateTime BatchDate = DateTime.Today;
            DateTime ActualTime = DateTime.Now;
            long ActualSize = new FileInfo(FilePath).Length;
            DateTime EarliestExpectedTime = DetermineEarliestExpectedTime();
            DateTime DeadlineTime = DetermineDeadlineTime();
            string Schedule = DetermineSchedule(FilePath);
            string Status = DetermineStatus(ActualTime, EarliestExpectedTime, DeadlineTime);

            InsertRecordIntoLookUpTable(FileName, FilePath, EarliestExpectedTime, DeadlineTime, Schedule);
            InsertRecordIntoTransactionalTable(BatchDate, FileName, FilePath, ActualTime, ActualSize, Status);
        }

        static void OnFileChanged(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"A file {e.Name} was updated at {e.FullPath}");
            
            string FileName = e.Name;
            string FilePath = e.FullPath;
            DateTime BatchDate = DateTime.Today;
            DateTime ActualTime = DateTime.Now;
            long ActualSize = new FileInfo(FilePath).Length;
            DateTime EarliestExpectedTime = DetermineEarliestExpectedTime();
            DateTime DeadlineTime = DetermineDeadlineTime();
            string Status = DetermineStatus(ActualTime, EarliestExpectedTime, DeadlineTime);


            UpdateRecordInTransactionalTable(FileName, FilePath, BatchDate, ActualTime, ActualSize, Status);
        }

        static void OnFileDeleted(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"A file {e.Name} was deleted from {e.FullPath}");
            string FileName = e.Name;
            string FilePath = e.FullPath;

            DeleteRecordFromTransactionalTable(FileName, FilePath);
        }

        static void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            Console.WriteLine($"A file {e.Name} was renamed to {e.FullPath}");
            string OldFileName = e.OldName;
            string OldFilePath = e.OldFullPath;
            string NewFileName = e.Name;
            string NewFilePath = e.FullPath;
            DateTime BatchDate = DateTime.Today;
            DateTime ActualTime = DateTime.Now;

            RenameRecordInTransactionalTable(NewFileName, NewFilePath, BatchDate, ActualTime, OldFileName, OldFilePath);

        }

        static void StartHourlyReportGenerator()
        {
            Timer hourlyTimer = new Timer();
            hourlyTimer.Interval = 60 * 1000;
            hourlyTimer.Elapsed += GenerateHourlyReport;
            hourlyTimer.Start();
        }

        static void GenerateHourlyReport(object sender, ElapsedEventArgs e)
        {
            DateTime BatchDate = DateTime.Now;
            string query = $"SELECT * FROM {transactionalTableName} WHERE BatchDate = '{BatchDate:yyyy-MM-dd}' AND Status = 'overdue' ";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {

                using (SqlCommand command = new SqlCommand(query, connection))
                { 
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    Console.WriteLine($"Hourly report for {BatchDate:yyyy-MM-dd}: ");

                    if (!reader.HasRows)
                    {
                        Console.WriteLine("There are no overdue files for this hour.");
                    }

                    else
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine($"File Name: {reader["FileName"]}, File Path: {reader["FilePath"]}. Need Attention!!! ");
                        }
                    }

                reader.Close();
                }

            }
        }

        static DateTime DetermineEarliestExpectedTime()
        {
            return DateTime.Today.AddHours(09).AddMinutes(00);
        }

        static DateTime DetermineDeadlineTime()
        {
            DateTime DeadlineTime = DateTime.Today.AddHours(13).AddMinutes(48);
            return DeadlineTime;
        }

        static string DetermineSchedule(string FilePath)
        {
            if (FilePath.Contains("weekly"))
            {
                return "Weekly Friday";
            }
            else if (FilePath.Contains("monthly"))
            {
                return "monthly Business day or Calender day";
            }
            else if (FilePath.Contains("yearly"))
            {
                return "yearly month Business day or Calender day";
            }
            else
            {
                return "daily Business day or calender day";
            }
        }

        static string DetermineStatus(DateTime ActualTime, DateTime EarliestExpectedTime, DateTime DeadlineTime)
        {
            if (ActualTime >= EarliestExpectedTime && ActualTime <= DeadlineTime)
            {
                return "due";
            }
            else if (ActualTime > DeadlineTime)
            {
                return "overdue";
            }
            else
            {
                return "published";
            }
        }

        static void InsertRecordIntoLookUpTable(string FileName, string FilePath, DateTime EarliestExpectedTime, DateTime DeadlineTime, string Schedule)
        {
            string query = $"INSERT INTO {lookupTableName} (FileName, FilePath, EarliestExpectedTime, DeadlineTime, Schedule) VALUES('{FileName}', '{FilePath}', '{EarliestExpectedTime}', '{DeadlineTime}', '{Schedule}')";
           
            using (SqlConnection connection = new SqlConnection(connectionString))
            {

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    connection.Open();
                    int v = command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }

        static void InsertRecordIntoTransactionalTable(DateTime BatchDate, string FileName, string FilePath, DateTime ActualTime, long ActualSize, string Status)
        {
            string query = $"INSERT INTO {transactionalTableName} (BatchDate, FileName, FilePath, ActualTime, ActualSize, Status) VALUES ( '{BatchDate:yyyy-MM-dd}', '{FileName}', '{FilePath}', '{ActualTime}', '{ActualSize}', '{Status}')";
            
            using (SqlConnection connection = new SqlConnection(connectionString))
            {

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    connection.Open();
                    int v = command.ExecuteNonQuery();
                    connection.Close();
                }   
            }
        }

        static void UpdateRecordInTransactionalTable(string FileName, string FilePath, DateTime BatchDate, DateTime ActualTime, long ActualSize, string Status)
        {
            
            string query = $"UPDATE {transactionalTableName} SET  FilePath = '{FilePath}', ActualTime = '{ActualTime}' , ActualSize = '{ActualSize}', Status = '{Status}'  WHERE BatchDate = '{BatchDate:yyyy-MM-dd}' AND FileName = '{FileName}' ";
            

            using (SqlConnection connection = new SqlConnection(connectionString))
            {

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                   
                    connection.Open();
                    int v =command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }

        static void DeleteRecordFromTransactionalTable(string FileName, string FilePath)
        {
            string query = $"DELETE FROM {transactionalTableName} WHERE FileName = '{FileName}' AND FilePath = '{FilePath}' ";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    connection.Open();
                    int v = command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }

        static void RenameRecordInTransactionalTable(string NewFileName, string NewFilePath, DateTime BatchDate, DateTime ActualTime, string OldFileName, string OldFilePath)
        {
            string query = $"UPDATE {transactionalTableName} SET FileName = '{NewFileName}', FilePath = '{NewFilePath}', BatchDate = '{BatchDate:yyyy-MM-dd}', ActualTime = '{ActualTime}' WHERE FileName = '{OldFileName}' AND FilePath = '{OldFilePath}' ";
           
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    connection.Open();
                    int v = command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }
    }
}
