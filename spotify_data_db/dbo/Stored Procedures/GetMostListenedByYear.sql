-- Create the stored procedure in the specified schema
CREATE   PROCEDURE [dbo].[GetMostListenedByYear]
    @Count /*parameter name*/ int /*datatype_for_param1*/ = 100, /*default_value_for_param1*/
    @Year /*parameter name*/ int /*datatype_for_param1*/
-- add more stored procedure parameters here
AS
BEGIN
    select top(@Count) year(d.EndTime), s.[Name], sum(cast(d.ms_played as bigint)) as [SumListened], min(d.EndTime)
    from dbo.[Data] as d
    join dbo.Song as s on s.ID = d.SongID
    where year(d.EndTime) = @Year
    group by year(d.EndTime), d.SongID, s.[Name]
    order by sum(cast(d.ms_played as bigint)) desc
END