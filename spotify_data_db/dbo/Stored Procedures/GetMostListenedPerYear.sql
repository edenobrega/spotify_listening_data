CREATE PROCEDURE dbo.GetMostListenedPerYear
AS
BEGIN
select t.[Year], s.[Name], t.SumListened
from (
SELECT
    year(d.EndTime) as [Year], 
    d.songid, 
    sum(cast(d.ms_played as bigint)) as [SumListened],
    t_index = ROW_NUMBER() OVER(PARTITION BY year(d.EndTime) ORDER BY sum(cast(d.ms_played as bigint)) desc)
FROM dbo.[Data] as d
join dbo.Song as s on s.ID = d.SongID
group by year(d.EndTime)
,d.SongID
) as t
join dbo.Song as s on s.ID = t.SongID
where t_index = 1


END