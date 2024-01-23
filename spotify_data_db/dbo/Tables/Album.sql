﻿CREATE TABLE [dbo].[Album]
(
	[ID] INT NOT NULL PRIMARY KEY IDENTITY, 
    [Name] NVARCHAR(1000) NULL, 
    [ArtistID] INT NULL, 
    CONSTRAINT [FK_Album_ToArtist] FOREIGN KEY ([ArtistID]) REFERENCES [Artist]([ID]) 
)
