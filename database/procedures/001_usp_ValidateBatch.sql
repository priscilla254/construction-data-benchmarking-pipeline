/*
Minimal validation procedure for the benchmarking POC.

Current responsibility:
- ensure the batch exists
- clear prior procedure-generated validation rows
- log a few basic staging-level validation errors

The Python ingestion engine already performs extensive workbook validation before
calling this procedure, so this proc is intentionally lightweight for now.
*/

CREATE OR ALTER PROCEDURE stg.usp_ValidateBatch
    @LoadBatchID UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (
        SELECT 1
        FROM stg.LoadBatch
        WHERE LoadBatchID = @LoadBatchID
    )
    BEGIN
        THROW 50001, 'Load batch not found.', 1;
    END;

    DELETE FROM stg.ValidationError
    WHERE LoadBatchID = @LoadBatchID
      AND ErrorType IN (
          'PROC_MISSING_PROJECT_INFORMATION',
          'PROC_MISSING_LEVEL2',
          'PROC_MISSING_L3',
          'PROC_UNKNOWN_SECTOR'
      );

    IF NOT EXISTS (
        SELECT 1
        FROM stg.ProjectInformation
        WHERE LoadBatchID = @LoadBatchID
    )
    BEGIN
        INSERT INTO stg.ValidationError (
            LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity
        )
        VALUES (
            @LoadBatchID, 'ProjectInformation', NULL, NULL,
            'PROC_MISSING_PROJECT_INFORMATION',
            'No staged ProjectInformation rows were found for this batch.',
            'ERROR'
        );
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM stg.Level2
        WHERE LoadBatchID = @LoadBatchID
    )
    BEGIN
        INSERT INTO stg.ValidationError (
            LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity
        )
        VALUES (
            @LoadBatchID, 'Level2', NULL, NULL,
            'PROC_MISSING_LEVEL2',
            'No staged Level2 rows were found for this batch.',
            'ERROR'
        );
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM stg.LineItem_L3
        WHERE LoadBatchID = @LoadBatchID
    )
    BEGIN
        INSERT INTO stg.ValidationError (
            LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity
        )
        VALUES (
            @LoadBatchID, 'LineItem_L3', NULL, NULL,
            'PROC_MISSING_L3',
            'No staged LineItem_L3 rows were found for this batch.',
            'ERROR'
        );
    END;

    INSERT INTO stg.ValidationError (
        LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity
    )
    SELECT
        pi.LoadBatchID,
        'ProjectInformation',
        pi.RowNum,
        'SectorCode',
        'PROC_UNKNOWN_SECTOR',
        CONCAT('SectorCode ''', pi.SectorCode, ''' was not found in dbo.DimSector.'),
        'ERROR'
    FROM stg.ProjectInformation pi
    LEFT JOIN dbo.DimSector ds
        ON UPPER(LTRIM(RTRIM(ds.SectorCode))) = UPPER(LTRIM(RTRIM(pi.SectorCode)))
    WHERE pi.LoadBatchID = @LoadBatchID
      AND pi.SectorCode IS NOT NULL
      AND ds.SectorKey IS NULL;

    UPDATE lb
    SET ErrorCount = x.ErrorCount
    FROM stg.LoadBatch lb
    CROSS APPLY (
        SELECT COUNT(*) AS ErrorCount
        FROM stg.ValidationError ve
        WHERE ve.LoadBatchID = lb.LoadBatchID
          AND ve.Severity = 'ERROR'
    ) x
    WHERE lb.LoadBatchID = @LoadBatchID;
END;
GO
