

CREATE   PROCEDURE dbo.GetTopListenedSongs
    @FilterAmount int = -1
AS
BEGIN
    if @FilterAmount = -1
    begin
        set @FilterAmount = (select count(1) from dbo.[Data] as d join dbo.Song as s on s.ID = d.SongID)
    end

    select top(@FilterAmount) s.Name, sum(d.ms_played) as [Sum Listening Time (ms)], min(d.EndTime) as [First Listen], max(d.EndTime) as [End Time]
    from dbo.[Data] as d
    join dbo.Song as s on s.ID = d.SongID
    group by s.Name
    order by sum(d.ms_played) desc
END