/*
Run this script in the target SQL Server database before using the backend.

It creates:
- stg schema
- lookup table used by sector resolution
- load batch / validation tables
- staging tables expected by ingestion_engine/excel_file_ingestion.py
*/

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA stg');
END;
GO

IF OBJECT_ID('dbo.DimSector', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.DimSector (
        SectorKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        SectorCode NVARCHAR(50) NOT NULL,
        SectorName NVARCHAR(255) NOT NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_DimSector_SectorCode' AND object_id = OBJECT_ID('dbo.DimSector'))
BEGIN
    CREATE UNIQUE INDEX UX_DimSector_SectorCode
        ON dbo.DimSector (SectorCode);
END;
GO

IF OBJECT_ID('stg.LoadBatch', 'U') IS NULL
BEGIN
    CREATE TABLE stg.LoadBatch (
        LoadBatchID UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        SourceFileName NVARCHAR(260) NULL,
        SourceFilePath NVARCHAR(1000) NULL,
        BatchStatus NVARCHAR(50) NOT NULL,
        ErrorCount INT NOT NULL CONSTRAINT DF_LoadBatch_ErrorCount DEFAULT (0),
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_LoadBatch_CreatedAt DEFAULT (SYSUTCDATETIME())
    );
END;
GO

IF OBJECT_ID('stg.ValidationError', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ValidationError (
        ValidationErrorID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        SheetName NVARCHAR(255) NULL,
        RowNum INT NULL,
        ColumnName NVARCHAR(255) NULL,
        ErrorType NVARCHAR(100) NOT NULL,
        ErrorMessage NVARCHAR(1000) NOT NULL,
        Severity NVARCHAR(20) NOT NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_ValidationError_CreatedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_ValidationError_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.ProjectInformation', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ProjectInformation (
        StageProjectInformationID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        ProjectID NVARCHAR(100) NULL,
        ProjectName NVARCHAR(255) NULL,
        ClientName NVARCHAR(255) NULL,
        LocationLabel NVARCHAR(255) NULL,
        SectorCode NVARCHAR(50) NULL,
        CostStage NVARCHAR(100) NULL,
        BudgetStage NVARCHAR(100) NULL,
        SelectedContractor NVARCHAR(255) NULL,
        DataStatus NVARCHAR(100) NULL,
        Demolition BIT NULL,
        NewBuild BIT NULL,
        Refurbishment BIT NULL,
        HorizontalExtension BIT NULL,
        VerticalExtension BIT NULL,
        Basement BIT NULL,
        Asbestos BIT NULL,
        Contamination BIT NULL,
        BaseDate DATE NULL,
        Currency NVARCHAR(20) NULL,
        ProgrammeLengthInWeeks INT NULL,
        ProgrammeType NVARCHAR(100) NULL,
        GIFA DECIMAL(18,2) NULL,
        Notes NVARCHAR(MAX) NULL,
        CONSTRAINT FK_ProjectInformation_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.ProjectQuants', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ProjectQuants (
        StageProjectQuantID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        ProjectQuantCode NVARCHAR(100) NULL,
        ProjectQuantName NVARCHAR(255) NULL,
        Qty DECIMAL(18,4) NULL,
        Unit NVARCHAR(50) NULL,
        Comment NVARCHAR(1000) NULL,
        CONSTRAINT FK_ProjectQuants_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.ElementQuants_L2', 'U') IS NULL
BEGIN
    CREATE TABLE stg.ElementQuants_L2 (
        StageElementQuantL2ID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        L2Code NVARCHAR(100) NULL,
        L2Name NVARCHAR(255) NULL,
        Qty DECIMAL(18,4) NULL,
        Unit NVARCHAR(50) NULL,
        Comment NVARCHAR(1000) NULL,
        CONSTRAINT FK_ElementQuantsL2_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.Level2', 'U') IS NULL
BEGIN
    CREATE TABLE stg.Level2 (
        StageLevel2ID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        L1Code NVARCHAR(50) NULL,
        L1Name NVARCHAR(255) NULL,
        L2Code NVARCHAR(100) NULL,
        L2Name NVARCHAR(255) NULL,
        Rate DECIMAL(18,2) NULL,
        TotalCost DECIMAL(18,2) NOT NULL,
        CONSTRAINT FK_Level2_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.LineItem_L3', 'U') IS NULL
BEGIN
    CREATE TABLE stg.LineItem_L3 (
        StageLineItemL3ID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        L2Code NVARCHAR(100) NULL,
        L2Name NVARCHAR(255) NULL,
        LineID NVARCHAR(100) NULL,
        DisplayOrder INT NULL,
        ItemDescription NVARCHAR(1000) NULL,
        Quantity DECIMAL(18,4) NULL,
        Unit NVARCHAR(50) NULL,
        Rate DECIMAL(18,2) NULL,
        TotalCost DECIMAL(18,2) NULL,
        RowType NVARCHAR(50) NULL,
        CONSTRAINT FK_LineItemL3_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF OBJECT_ID('stg.Adjustments', 'U') IS NULL
BEGIN
    CREATE TABLE stg.Adjustments (
        StageAdjustmentID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        LoadBatchID UNIQUEIDENTIFIER NOT NULL,
        RowNum INT NOT NULL,
        SourceFileName NVARCHAR(260) NULL,
        AdjCategory NVARCHAR(100) NULL,
        AdjSubType NVARCHAR(100) NULL,
        Amount DECIMAL(18,2) NULL,
        Method NVARCHAR(50) NULL,
        RatePercent DECIMAL(18,4) NULL,
        AppliedToBase BIT NULL,
        IncludedInComparison BIT NULL,
        CONSTRAINT FK_Adjustments_LoadBatch
            FOREIGN KEY (LoadBatchID) REFERENCES stg.LoadBatch (LoadBatchID)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.DimSector)
BEGIN
    INSERT INTO dbo.DimSector (SectorCode, SectorName)
    VALUES
        ('EDU', 'Education'),
        ('HEALTH', 'Healthcare'),
        ('RESI', 'Residential'),
        ('COMM', 'Commercial'),
        ('IND', 'Industrial');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_LoadBatch_BatchStatus' AND object_id = OBJECT_ID('stg.LoadBatch'))
BEGIN
    CREATE INDEX IX_LoadBatch_BatchStatus
        ON stg.LoadBatch (BatchStatus, CreatedAt DESC);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ValidationError_LoadBatchID' AND object_id = OBJECT_ID('stg.ValidationError'))
BEGIN
    CREATE INDEX IX_ValidationError_LoadBatchID
        ON stg.ValidationError (LoadBatchID, Severity, SheetName);
END;
GO
