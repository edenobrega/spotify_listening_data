CREATE TABLE [dbo].[Song]
(
	[ID] INT NOT NULL PRIMARY KEY IDENTITY, 
    [Name] NVARCHAR(1000) NULL, 
    [ArtistID] INT NULL, 
    [AlbumID] INT NULL, 
    [Track_Uri] NVARCHAR(100) NULL, 
    CONSTRAINT [FK_Song_ToArtist] FOREIGN KEY ([ArtistID]) REFERENCES [Artist]([ID]) 
)
