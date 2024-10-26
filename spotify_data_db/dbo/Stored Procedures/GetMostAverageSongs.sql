CREATE   PROCEDURE [dbo].[GetMostAverageSongs]
    @ListenCountThreshold int = 1
AS
BEGIN
    declare @A float,
            @B float,
            @C float,
            @D float,
            @E float,
            @F float,
            @G float,
            @H float,
            @I float

    select @A = avg(Acousticness) 
        ,@B = avg(Danceability)
        ,@C = avg(Energy)
        ,@D = avg(Instrumentalness)
        ,@E = avg(Liveness)
        ,@F = avg(Loudness)
        ,@G = avg(Speechiness)
        ,@H = avg(Tempo)
        ,@I = avg(Valence)
    from dbo.AudioFeature

    declare @TrackdoneID int = (select ID from dbo.Reason where [Name] = 'trackdone')
    declare @FullyListenedSongs table(ID INT)

    insert into @FullyListenedSongs 
    select d.SongID
    from dbo.[Data] as d
    where d.ReasonEnd = @TrackdoneID
    group by d.SongID
    having count(1) > @ListenCountThreshold

    select TOP(10) s.ID AS [SongID] ,a.Name as [AritstName], s.Name as [SongName], s.Duration_ms
    from dbo.AudioFeature as af
    join dbo.Song as s on s.ID = af.SongID
    JOIN dbo.SongToArtist AS sta ON sta.SongID = s.ID AND sta.[Primary] = 1
    join dbo.Artist as a on a.ID = sta.ArtistID
    where af.SongID in (select * from @FullyListenedSongs)
    order by ABS(Acousticness - @A)
    ,ABS(Danceability - @B)
    ,ABS(Energy - @C)
    ,ABS(Instrumentalness - @D)
    ,ABS(Liveness - @E)
    ,ABS(Loudness - @F)
    ,ABS(Speechiness - @G)
    ,ABS(Tempo - @H)
    ,ABS(Valence - @I)
END