
CREATE   PROCEDURE [dbo].[GetMostListenedByYear]
    @Count int  = 100,
    @Year int 

AS
BEGIN
    select top(@Count) year(d.EndTime), s.[Name], sum(cast(d.ms_played as bigint)) as [SumListened], min(d.EndTime)
    from dbo.[Data] as d
    join dbo.Song as s on s.ID = d.SongID
    where year(d.EndTime) = @Year
    group by year(d.EndTime), d.SongID, s.[Name]
    order by sum(cast(d.ms_played as bigint)) desc
END