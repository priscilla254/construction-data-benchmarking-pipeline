/*
Minimal commit procedure for the benchmarking POC.

Current responsibility:
- ensure the batch exists
- stop commit if blocking validation errors exist

This is a placeholder for future movement from staging into curated/final tables.
*/

CREATE OR ALTER PROCEDURE stg.usp_CommitBatch
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
        THROW 50002, 'Load batch not found.', 1;
    END;

    IF EXISTS (
        SELECT 1
        FROM stg.ValidationError
        WHERE LoadBatchID = @LoadBatchID
          AND Severity = 'ERROR'
    )
    BEGIN
        THROW 50003, 'Batch has validation errors and cannot be committed.', 1;
    END;

    /*
    Future implementation point:
    - insert/update final fact and dimension tables here
    - record audit metadata for the committed batch
    */
END;
GO
