CREATE   PROCEDURE [dbo].[GetArtistsPercentOfTotalListened]
    @threshold BIGINT = 3600000
AS
BEGIN
    DROP TABLE IF EXISTS #tmp

    DECLARE @sumListened BIGINT
    SELECT @sumListened = SUM(CAST(ms_played AS BIGINT))
    FROM dbo.[Data]

    SELECT a.[Name], 
        SUM(d.ms_played) AS [Sum Listening Time (ms)],
        -- CAST(CAST(CAST(SUM(d.ms_played) AS DECIMAL(38, 16)) / cast(@sumListened AS DECIMAL(38, 16)) AS DECIMAL(38, 16)) * 100 AS NVARCHAR(25)) + '%' AS [Percent Of Total Listen], 
        CAST(CAST(SUM(d.ms_played) AS DECIMAL(38, 16)) / cast(@sumListened AS DECIMAL(38, 16)) AS DECIMAL(38, 16)) * 100 AS [Percent Of Total Listen], 
        MIN(d.EndTime) AS [First Listen],
        MAX(d.EndTime) AS [Last Listen]
    INTO #tmp
    FROM [dbo].[Data] AS d
    JOIN [dbo].[Song] AS s ON s.ID = d.[SongID]
    JOIN [dbo].[SongToArtist] AS sta ON sta.SongID = s.ID AND sta.[Primary] = 1
    JOIN [dbo].[Artist] AS a ON a.ID = sta.ArtistID
    GROUP BY a.[ID], a.[Name]
    HAVING SUM(d.[ms_played]) > @threshold

    SELECT *
    FROM 
    (
        SELECT t.[Name],
            t.[Sum Listening Time (ms)],
            CAST(t.[Percent Of Total Listen] AS NVARCHAR(25)) + '%' AS [Percent Of Total Listen],
            t.[First Listen],
            t.[Last Listen]
        FROM #tmp AS t

        UNION

        SELECT 'Below Threshold' AS [Name],
            @sumListened - SUM(CAST(t.[Sum Listening Time (ms)] AS BIGINT)) AS [Sum Listening Time (ms)], 
            CAST(100 - SUM(t.[Percent Of Total Listen]) AS NVARCHAR(25)) + '%' AS [Percent Of Total Listen],
            MIN([First Listen]) AS [First Listen],
            MAX([Last Listen]) AS [Last Listen]
        FROM #tmp AS t
    ) AS o
    ORDER BY o.[Sum Listening Time (ms)] DESC

END