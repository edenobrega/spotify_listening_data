CREATE TABLE [dbo].[SongToArtist] (
    [Primary]  BIT NOT NULL,
    [SongID]   INT NOT NULL,
    [ArtistID] INT NOT NULL,
    CONSTRAINT [FK_SongToArtist_Artist] FOREIGN KEY ([ArtistID]) REFERENCES [dbo].[Artist] ([ID]),
    CONSTRAINT [FK_SongToArtist_Song] FOREIGN KEY ([SongID]) REFERENCES [dbo].[Song] ([ID])
);

