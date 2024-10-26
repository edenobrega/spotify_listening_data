
CREATE PROCEDURE [dbo].[GetYearlyDataStats]
    @Year int = -1 
AS
BEGIN

IF @Year = -1
BEGIN
    select year(d.EndTime) as [Year],
            sum(cast(d.ms_played as bigint)) as [Miliseconds Sum],
            sum(cast(d.ms_played as bigint)) / 60000 as [Minutes Sum],
            sum(cast(d.ms_played as bigint)) / 3600000 as [Hours Sum],
            sum(cast(d.ms_played as bigint)) / 86400000 as [Days Sum],
            count(distinct s.Track_Uri) as [Unique Tracks Listened To],
            count(distinct sta.ArtistID) as [Artists Listened To],
            count(distinct s.AlbumID) as [Albums Listened To]
    from dbo.[Data] as d
    join dbo.[Song] as s on s.ID = d.SongID
    JOIN dbo.SongToArtist AS sta ON sta.SongID = s.ID AND sta.[Primary] = 1
    group by year(d.EndTime)
    order by year(d.EndTime)
END
ELSE
BEGIN
    select year(d.EndTime) as [Year],
            sum(cast(d.ms_played as bigint)) as [Miliseconds Sum],
            sum(cast(d.ms_played as bigint)) / 60000 as [Minutes Sum],
            sum(cast(d.ms_played as bigint)) / 3600000 as [Hours Sum],
            sum(cast(d.ms_played as bigint)) / 86400000 as [Days Sum],
            count(distinct s.Track_Uri) as [Unique Tracks Listened To],
            count(distinct sta.ArtistID) as [Artists Listened To],
            count(distinct s.AlbumID) as [Albums Listened To]
    from dbo.[Data] as d
    join dbo.[Song] as s on s.ID = d.SongID
    JOIN dbo.SongToArtist AS sta ON sta.SongID = s.ID AND sta.[Primary] = 1
    where year(d.EndTime) = @Year
    group by year(d.EndTime)
    order by year(d.EndTime)
END

END