CREATE TABLE [dbo].[Song] (
    [ID]        INT             IDENTITY (1, 1) NOT NULL,
    [Name]      NVARCHAR (1000) NULL,
    [AlbumID]   INT             NULL,
    [Track_Uri] NVARCHAR (100)  NULL,
    [Duration]  INT             NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC),
    CONSTRAINT [FK_Song_ToAlbum] FOREIGN KEY ([AlbumID]) REFERENCES [dbo].[Album] ([ID])
);


