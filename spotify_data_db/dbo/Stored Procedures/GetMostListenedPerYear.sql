
CREATE PROCEDURE [dbo].[GetMostListenedPerYear]
AS
BEGIN
    SELECT UserID, t.[Year], a.Name AS [Artist Name] ,s.[Name] AS [Song Name], t.SumListened
    FROM (
        SELECT
            d.UserID,
            YEAR(d.EndTime) as [Year], 
            d.songid, 
            SUM(CAST(d.ms_played AS BIGINT)) / 60000 AS [SumListened],
            t_index = ROW_NUMBER() OVER(PARTITION BY year(d.EndTime), d.UserID ORDER BY SUM(CAST(d.ms_played AS BIGINT)) DESC)
        FROM dbo.[Data] AS d
        JOIN dbo.Song AS s ON s.ID = d.SongID
        GROUP BY YEAR(d.EndTime), d.SongID, d.UserID
    ) AS t
    JOIN dbo.Song AS s ON s.ID = t.SongID
    JOIN dbo.SongToArtist AS sta ON sta.SongID = s.ID AND STA.[Primary] = 1 
    JOIN dbo.Artist AS a ON a.ID = sta.ArtistID
    WHERE t_index = 1
    ORDER BY UserID DESC, [Year] DESC;
END