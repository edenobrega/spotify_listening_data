
CREATE PROCEDURE dbo.GetYearlyDataStats
AS
BEGIN
select year(d.EndTime) as [Year],
        sum(cast(d.ms_played as bigint)) as [Miliseconds Sum],
        sum(cast(d.ms_played as bigint)) / 60000 as [Minutes Sum],
        sum(cast(d.ms_played as bigint)) / 3600000 as [Hours Sum],
        sum(cast(d.ms_played as bigint)) / 86400000 as [Days Sum],
        count(distinct s.Track_Uri) as [Unique Tracks Listened To],
        count(distinct s.ArtistID) as [Artists Listened To],
        count(distinct s.AlbumID) as [Albums Listened To]
from dbo.[Data] as d
join dbo.[Song] as s on s.ID = d.SongID
group by year(d.EndTime)
order by year(d.EndTime)
END