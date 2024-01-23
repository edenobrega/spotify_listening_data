CREATE TABLE [dbo].[AudioFeature] (
    [SongID]           INT        NOT NULL,
    [Duration]         INT        NULL,
    [Acousticness]     FLOAT (53) NOT NULL,
    [Danceability]     FLOAT (53) NOT NULL,
    [Energy]           FLOAT (53) NOT NULL,
    [Instrumentalness] FLOAT (53) NOT NULL,
    [Key]              INT        NOT NULL,
    [Liveness]         FLOAT (53) NOT NULL,
    [Loudness]         FLOAT (53) NOT NULL,
    [Mode]             INT        NOT NULL,
    [Speechiness]      FLOAT (53) NOT NULL,
    [Tempo]            FLOAT (53) NOT NULL,
    [TimeSignature]    INT        NOT NULL,
    [Valence]          FLOAT (53) NOT NULL
);

