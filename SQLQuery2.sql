create table LookUpTable 
(FileName varchar(50) not null, FilePath varchar(100) not null, 
EarliestExceptedTime time not null, DeadlineTime time not null, 
Schedule varchar(50) not null, primary key (FileName, FilePath));

CREATE TABLE TransactionalTable (
  BatchDate DATE NOT NULL, 
  FileName VARCHAR(50) NOT NULL, 
  FilePath VARCHAR(100) NOT NULL,
  ActualTime TIME NOT NULL, 
  ActualSize BIGINT NOT NULL, 
  Status VARCHAR(10) NOT NULL,
  FOREIGN KEY (FileName, FilePath) REFERENCES LookUpTable (FileName, FilePath),
  PRIMARY KEY (BatchDate, FileName, FilePath) 
);