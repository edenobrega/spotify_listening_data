CREATE PROCEDURE dbo.GetMonthlyListenData
AS
BEGIN
    select year(d.EndTime) as [Year],
            month(d.EndTime) as [Month],
            sum(cast(d.ms_played as bigint)) as [Miliseconds],
            sum(cast(d.ms_played as bigint)) / 60000 as [Minutes],
            sum(cast(d.ms_played as bigint)) / 3600000 as [Hours],
            sum(cast(d.ms_played as bigint)) / 86400000 as [Days],
            count(distinct s.Track_Uri) as [Unique_Tracks],
            count(distinct s.AlbumID) as [Albums_Listened],
            count(distinct s.ArtistID) as [Artists_Listened]
    from dbo.[Data] as d
    join dbo.[Song] as s on s.ID = d.SongID
    group by year(d.EndTime), month(d.EndTime)
    order by year(d.EndTime), month(d.EndTime)
END