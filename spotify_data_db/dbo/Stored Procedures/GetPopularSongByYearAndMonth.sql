CREATE PROCEDURE dbo.GetPopularSongByYearAndMonth
AS
BEGIN
    select t.[Year], t.[Month], s.Name, t.SumListened
    from (
    select 
        year(d.EndTime) as [Year],
        month(d.EndTime) as [Month],
        d.[SongID] as [SongID],
        sum(d.ms_played) as [SumListened],
        s_index = ROW_NUMBER() OVER(PARTITION BY year(d.EndTime), month(d.EndTime) ORDER BY sum(d.ms_played) desc)
    from dbo.[Data] as d
    group by year(d.EndTime), month(d.EndTime), d.[SongID]
    ) as t
    join dbo.[Song] as s on s.ID = t.SongID
    where t.s_index = 1
END