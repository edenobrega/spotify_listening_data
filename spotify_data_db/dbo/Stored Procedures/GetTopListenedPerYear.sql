-- Create the stored procedure in the specified schema
CREATE PROCEDURE dbo.GetTopListenedPerYear
AS
BEGIN
    select t.[Year], s.[Name], t.SumListened
    from (
        SELECT
            year(d.EndTime) as [Year], 
            d.songid, 
            sum(cast(d.ms_played as bigint)) as [SumListened],
            s_index = ROW_NUMBER() OVER(PARTITION BY year(d.EndTime) ORDER BY sum(cast(d.ms_played as bigint)) desc),
            t_index = DENSE_RANK() OVER (ORDER BY year(d.EndTime))
        FROM dbo.[Data] as d
        join dbo.Song as s on s.ID = d.SongID
        group by year(d.EndTime), d.SongID
    ) as t
    join dbo.Song as s on s.ID = t.SongID
    where s_index = 1
END