CREATE TABLE [dbo].[Data] (
    [ID]               INT           IDENTITY (1, 1) NOT NULL,
    [FileID]           INT           NOT NULL,
    [EndTime]          DATETIME2 (7) NOT NULL,
    [ms_played]        INT           NOT NULL,
    [ReasonStart]      INT           NOT NULL,
    [ReasonEnd]        INT           NOT NULL,
    [PlatformID]       INT           NOT NULL,
    [SongID]           INT           NOT NULL,
    [Country]          NVARCHAR (2)  NOT NULL,
    [Skipped]          BIT           NULL,
    [Offline]          BIT           NULL,
    [OfflineTimestamp] DATETIME2 (7) NULL,
    [IncognitoMode]    BIT           NULL,
    [Shuffle]          BIT           NOT NULL,
    [UserID]           INT           NOT NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC),
    CONSTRAINT [FK_Data_ToReasonEnd] FOREIGN KEY ([ReasonEnd]) REFERENCES [dbo].[Reason] ([ID]),
    CONSTRAINT [FK_Data_ToReasonStart] FOREIGN KEY ([ReasonStart]) REFERENCES [dbo].[Reason] ([ID]),
    CONSTRAINT [FK_Data_ToSong] FOREIGN KEY ([SongID]) REFERENCES [dbo].[Song] ([ID])
);


