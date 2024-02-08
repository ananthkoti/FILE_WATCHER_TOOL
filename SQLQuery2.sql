create table LookUpTable 
(FileName varchar(50) not null, FilePath varchar(100) not null, 
EarliestExceptedTime time not null, DeadlineTime time not null, 
Schedule varchar(50) not null, primary key (FileName, FilePath));


create table TransactionalTable 
(BatchDate date not null, FileName varchar(50) not null, 
FilePath varchar(100) not null, ActualTime time not null, 
ActualSize bigint not null, Status varchar(10) not null, foreign key (FileName, FilePath)
references LookUpTable (FileName, FilePath), primary key (BatchDate, FileName, FilePath));