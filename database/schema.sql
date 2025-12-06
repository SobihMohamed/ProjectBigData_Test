CREATE DATABASE EnergyDb;
GO

USE EnergyDB;
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
    Peak_Hours_Flag INT,           
    Temp_Status NVARCHAR(50),      
    Record_Inserted_At DATETIME DEFAULT GETDATE()
);

CREATE TABLE HouseTotalLoad (
    House_ID NVARCHAR(50),
    Total_Load FLOAT,
    Batch_Time DATETIME DEFAULT GETDATE()
);