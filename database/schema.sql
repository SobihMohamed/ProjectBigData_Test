CREATE DATABASE EnergyDb;
GO

USE EnergyDb;
GO

CREATE TABLE DeviceStats (
    Device NVARCHAR(50),
    Avg_Power FLOAT,
    Max_Consumption FLOAT,
    Min_Consumption FLOAT,
    Batch_Time DATETIME DEFAULT GETDATE() 
);

CREATE TABLE AlertsLog (
    EventTime DATETIME,
    House_ID NVARCHAR(50),
    Device NVARCHAR(50),
    Power_Usage_W FLOAT,
    Sudden_Increase_Flag INT,
    Peak_Hours_Flag INT
    Record_Inserted_At DATETIME DEFAULT GETDATE(),
);