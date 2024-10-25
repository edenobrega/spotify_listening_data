CREATE TABLE [dbo].[Artist] (
    [ID]   INT             IDENTITY (1, 1) NOT NULL,
    [Name] NVARCHAR (1000) NULL,
    [URI]  NVARCHAR (100)  NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC)
);


