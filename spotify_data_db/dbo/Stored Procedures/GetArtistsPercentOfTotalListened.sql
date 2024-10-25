CREATE PROCEDURE [dbo].[GetArtistsPercentOfTotalListened]
AS
BEGIN
    declare @sumListened bigint
    select @sumListened = sum(cast(ms_played as bigint))
    from dbo.[Data]

    select a.[Name], 
    sum(d.ms_played) / 60000 as [Sum Listening Time (minutes)],
    cast(cast(cast(sum(d.ms_played) as decimal(38, 16)) / cast(@sumListened as decimal(38, 16)) as decimal(38, 16)) * 100 as nvarchar(25)) + '%' as [Percent Of Total Listen], 
    min(d.EndTime) as [First Listen],
    max(d.EndTime) as [Last Time] 
    from dbo.[Data] as d
    join dbo.Song as s on s.ID = d.SongID
    join dbo.Artist as a on a.ID = s.ArtistID
    group by a.ID, a.[Name]
    order by sum(d.ms_played) desc

    -- declare @teehee table ([data] decimal(38,27))
    
    -- insert into @teehee
    -- select 
    -- cast(cast(cast(sum(d.ms_played) as decimal(38, 16)) / cast(@sumListened as decimal(38, 16)) as decimal(38, 16)) * 100 as nvarchar(25)) as [Percent Of Total Listen]
    -- from dbo.[Data] as d
    -- join dbo.Song as s on s.ID = d.SongID
    -- join dbo.Artist as a on a.ID = s.ArtistID
    -- group by a.ID, a.[Name]

    -- select sum([data]) from @teehee
END