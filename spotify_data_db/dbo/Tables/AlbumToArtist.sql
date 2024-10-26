CREATE TABLE [dbo].[AlbumToArtist] (
    [Primary]  BIT NOT NULL,
    [AlbumID]  INT NOT NULL,
    [ArtistID] INT NOT NULL,
    CONSTRAINT [FK_AlbumToArtist_Album] FOREIGN KEY ([AlbumID]) REFERENCES [dbo].[Album] ([ID]),
    CONSTRAINT [FK_AlbumToArtist_Artist] FOREIGN KEY ([ArtistID]) REFERENCES [dbo].[Artist] ([ID])
);



