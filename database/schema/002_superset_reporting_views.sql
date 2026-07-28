/*
Curated read models for Apache Superset dashboards.
These views avoid exposing all raw staging columns to BI users.
*/

IF OBJECT_ID('dbo.vw_BI_ProjectOverview', 'V') IS NOT NULL
    DROP VIEW dbo.vw_BI_ProjectOverview;
GO

CREATE VIEW dbo.vw_BI_ProjectOverview
AS
SELECT
    pi.LoadBatchID,
    lb.CreatedAt,
    pi.ProjectID,
    pi.ProjectName,
    pi.Notes AS SectorDescription,
    pi.LocationLabel,
    COUNT(pt.StageProjectTendererID) AS NumberOfResponses
FROM stg.ProjectInformation pi
INNER JOIN stg.LoadBatch lb
    ON lb.LoadBatchID = pi.LoadBatchID
LEFT JOIN stg.ProjectTenderer pt
    ON pt.LoadBatchID = pi.LoadBatchID
GROUP BY
    pi.LoadBatchID,
    lb.CreatedAt,
    pi.ProjectID,
    pi.ProjectName,
    pi.Notes,
    pi.LocationLabel;
GO

IF OBJECT_ID('dbo.vw_BI_TenderReview', 'V') IS NOT NULL
    DROP VIEW dbo.vw_BI_TenderReview;
GO

CREATE VIEW dbo.vw_BI_TenderReview
AS
SELECT
    pt.LoadBatchID,
    pi.ProjectID,
    pi.ProjectName,
    pt.TendererLabel,
    pt.TendererName,
    pt.IsSelected,
    pt.FinalAdjustedTenderSum,
    pt.ConstructionBudget,
    pt.VarianceToCostplan AS VarianceToConstructionBudget
FROM stg.ProjectTenderer pt
LEFT JOIN stg.ProjectInformation pi
    ON pi.LoadBatchID = pt.LoadBatchID;
GO

IF OBJECT_ID('dbo.vw_BI_Level2CostBreakdown', 'V') IS NOT NULL
    DROP VIEW dbo.vw_BI_Level2CostBreakdown;
GO

CREATE VIEW dbo.vw_BI_Level2CostBreakdown
AS
SELECT
    l2.LoadBatchID,
    pi.ProjectID,
    pi.ProjectName,
    l2.L1Name,
    l2.L2Name,
    l2.Rate,
    l2.TotalCost
FROM stg.Level2 l2
LEFT JOIN stg.ProjectInformation pi
    ON pi.LoadBatchID = l2.LoadBatchID;
GO

IF OBJECT_ID('dbo.vw_BI_AdjustmentSummary', 'V') IS NOT NULL
    DROP VIEW dbo.vw_BI_AdjustmentSummary;
GO

CREATE VIEW dbo.vw_BI_AdjustmentSummary
AS
SELECT
    a.LoadBatchID,
    pi.ProjectID,
    pi.ProjectName,
    a.AdjCategory,
    a.AdjSubType,
    SUM(COALESCE(a.Amount, 0)) AS TotalAmount
FROM stg.Adjustments a
LEFT JOIN stg.ProjectInformation pi
    ON pi.LoadBatchID = a.LoadBatchID
GROUP BY
    a.LoadBatchID,
    pi.ProjectID,
    pi.ProjectName,
    a.AdjCategory,
    a.AdjSubType;
GO
