CREATE PROCEDURE dbo.GetArtistsPercentOfTotalListened
AS
BEGIN
    declare @sumListened bigint
    select @sumListened = sum(cast(ms_played as bigint))
    from dbo.[Data]

    select a.[Name], 
    sum(d.ms_played) / 60000 as [Sum Listening Time (minutes)],
    cast(cast(cast(sum(d.ms_played) as decimal(18, 3)) / cast(@sumListened as decimal(18, 3)) as decimal(18, 3)) * 100 as nvarchar(25)) + '%' as [Percent Of Total Listen], 
    min(d.EndTime) as [First Listen],
    max(d.EndTime) as [Last Time] 
    from dbo.[Data] as d
    join dbo.Song as s on s.ID = d.SongID
    join dbo.Artist as a on a.ID = s.ArtistID
    group by a.ID, a.[Name]
    order by sum(d.ms_played) desc
END