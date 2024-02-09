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

         private static void Main(string[] args)
        {
          //  CreateLookUpTable();

          //  CreateTransactionalTable();

            StartFileMonitoring();

            StartHourlyReportGenerator();

            Console.WriteLine("File monitoring started. Press any key to exit.");
            Console.ReadKey();
        }

       /* static void CreateLookUpTable()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string query = $"CREATE TABLE IF NOT EXISTS {lookupTableName} (FileName varchar(50) not null, FilePath varchar(100) not null, EarliestExpectedTime time not null, DeadlineTime time not null, Schedule varchar(50) not null, primary key (FileName, FilePath))";
                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }
        static void CreateTransactionalTable()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string query = $"CREATE TABLE IF NOT EXISTS {transactionalTableName} (BatchDate date not null, FileName varchar(50) not null, FilePath varchar(100) not null, ActualTime time not null, ActualSize bigint not null, Status varchar(10) not null, foreign key (FileName, FilePath) references LookUpTable (FileName, FilePath), primary key (BatchDate, FileName, FilePath))";
                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }
       */
        static void StartFileMonitoring()
        {
            FileSystemWatcher watcher = new FileSystemWatcher();
            watcher.Path = @"C:\sample_file_watcher";
            watcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.LastAccess;
            watcher.Filter = "*.*";
            watcher.Created += OnFileCreated;
            watcher.Changed += OnFileChanged;
            watcher.Deleted += OnFileDeleted;
            watcher.Renamed += OnFileRenamed;
            watcher.EnableRaisingEvents = true;
        }
        static void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            string FileName = e.Name;
            string FilePath = e.FullPath;
            string BatchDate = DateTime.Today.ToString("yyyy-MM-dd");
            DateTime ActualTime = DateTime.Now;
            long ActualSize = new FileInfo(FilePath).Length;
            DateTime EarliestExpectedTime = DetermineEarliestExpectedTime(FilePath);
            DateTime DeadlineTime = DetermineDeadlineTime(FilePath);
            string Schedule = DetermineSchedule(FilePath);
            string Status = DetermineStatus(ActualTime, EarliestExpectedTime, DeadlineTime);

            InsertRecordIntoLookUpTable(FileName, FilePath, EarliestExpectedTime, DeadlineTime, Schedule);
            InsertRecordIntoTransactionalTable(BatchDate, FileName, FilePath, ActualTime, ActualSize, Status);
        }

        static void OnFileChanged(object sender, FileSystemEventArgs e)
        {
            string FileName = e.Name;
            string FilePath = e.FullPath;
            string BatchDate = DateTime.Today.ToString("yyyy-MM-dd");
            DateTime ActualTime = DateTime.Now;
            long ActualSize = new FileInfo(FilePath).Length;

            UpdateRecordInTransactionalTable(FileName, FilePath, BatchDate, ActualTime, ActualSize);
        }

        static void OnFileDeleted(object sender, FileSystemEventArgs e)
        {
            string FileName = e.Name;
            string FilePath = e.FullPath;

            DeleteRecordFromTransactionalTable(FileName, FilePath);
        }

        static void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            string OldFileName = e.OldName;
            string OldFilePath = e.OldFullPath;
            string NewFileName = e.Name;
            string NewFilePath = e.FullPath;
            string BatchDate = DateTime.Today.ToString("yyyy-MM-dd");
            DateTime ActualTime = DateTime.Now;

            RenameRecordInTransactionalTable(NewFileName, NewFilePath, BatchDate, ActualTime, OldFileName, OldFilePath);

        }

        static void StartHourlyReportGenerator()
        {
            Timer hourlyTimer = new Timer();
            hourlyTimer.Interval = 60 * 60 * 1000;
            hourlyTimer.Elapsed += GenerateHourlyReport;
            hourlyTimer.Start();
        }

        static void GenerateHourlyReport(object sender, ElapsedEventArgs e)
        {
            string currentDate = DateTime.Today.ToString("yyyy-MM-dd");

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"SELECT * FROM {transactionalTableName} WHERE BatchDate = '{currentDate}' AND Status = 'overdue' ";
                SqlCommand command = new SqlCommand(query, connection);

                SqlDataReader reader = command.ExecuteReader();
                Console.WriteLine($"Hourly report for {currentDate}: ");
                while (reader.Read())
                {
                    Console.WriteLine($"File Name: {reader["FileName"]}, File Path: {reader["FilePath"]}");
                }

                reader.Close();

            }
        }

        static DateTime DetermineEarliestExpectedTime(string FilePath)
        {
            if (FilePath.Contains("daily"))
            {
                return DateTime.Today.AddHours(9);
            }
            else
            {
                return DateTime.Today;
            }
        }

        static DateTime DetermineDeadlineTime(string FilePath)
        {
            DateTime DeadlineTime = DateTime.Today.AddHours(19);

            if (DateTime.Now >= DeadlineTime)
            {
                DeadlineTime = DateTime.Today.AddDays(1).AddHours(19);
            }

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
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"INSERT INTO {lookupTableName} (FileName, FilePath, EarliestExpectedTime, DeadlineTime, Schedule)"
                                + $"VALUES ('{FileName}', '{FilePath}', '{EarliestExpectedTime}', '{DeadlineTime}', '{Schedule}')";

                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }

        static void InsertRecordIntoTransactionalTable(string BatchDate, string FileName, string FilePath, DateTime ActualTime, long ActualSize, string Status)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"INSERT INTO {transactionalTableName} (BatchDate, FileName, FilePath, ActualTime, ActualSize, Status)"
                                + $"VALUES ('{BatchDate}', '{FileName}', '{FilePath}', '{ActualTime}', '{ActualSize}', '{Status}')";

                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();

            }
        }

        static void UpdateRecordInTransactionalTable(string FileName, string FilePath, string BatchDate, DateTime ActualTime, long ActualSize)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"UPDATE {transactionalTableName} SET ActualTime = '{ActualTime}', ActualSize = '{ActualSize}'  BatchDate = '{BatchDate}' WHERE FileName = '{FileName}' AND FilePath = '{FilePath}' ";

                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }

        static void DeleteRecordFromTransactionalTable(string FileName, string FilePath)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"DELETE FROM {transactionalTableName} WHERE FileName = '{FileName}' AND FilePath = '{FilePath}' ";

                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }

        static void RenameRecordInTransactionalTable(string NewFileName, string NewFilePath, string BatchDate, DateTime ActualTime, string OldFileName, string OldFilePath)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string query = $"UPDATE {transactionalTableName} SET FileName = '{NewFileName}', FilePath = '{NewFilePath}', BatchDate = '{BatchDate}', ActualTime = '{ActualTime}' WHERE FileName = '{OldFileName}' AND FilePath = '{OldFilePath}' ";

                SqlCommand command = new SqlCommand(query, connection);
                command.ExecuteNonQuery();
            }
        }
    }
}
