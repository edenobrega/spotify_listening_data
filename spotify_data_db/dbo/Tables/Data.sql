CREATE TABLE [dbo].[Data]
(
	[ID] INT NOT NULL PRIMARY KEY IDENTITY,
    [EndTime] DATETIME2 NOT NULL, 
    [ms_played] INT NOT NULL, 
    [ReasonStart] INT NOT NULL, 
    [ReasonEnd] INT NOT NULL, 
    [PlatformID] INT NOT NULL, 
    [SongID] INT NOT NULL, 
    [Country] NVARCHAR(2) NOT NULL, 
    [Skipped] BIT NULL, 
    [Offline] BIT NULL, 
    [OfflineTimestamp] DATETIME2 NULL, 
    [IncognitoMode] BIT NULL, 
    [Shuffle] BIT NOT NULL,
    [UserID] INT NOT NULL, 
    CONSTRAINT [FK_Data_ToSong] FOREIGN KEY ([SongID]) REFERENCES [Song]([ID]), 
    CONSTRAINT [FK_Data_ToReasonStart] FOREIGN KEY ([ReasonStart]) REFERENCES [Reason]([ID]), 
    CONSTRAINT [FK_Data_ToReasonEnd] FOREIGN KEY ([ReasonEnd]) REFERENCES [Reason]([ID])
)
