CREATE VIEW [dbo].[DataCount]
AS
SELECT 'Data' AS [Table], COUNT(1) AS [Count]
FROM dbo.[Data]

UNION ALL

SELECT 'Song' AS [Table], COUNT(1) AS [Count]
FROM dbo.Song

UNION ALL

SELECT 'Album' AS [Table], COUNT(1) AS [Count]
FROM dbo.Album

UNION ALL

SELECT 'Artist' AS [Table], COUNT(1) AS [Count]
FROM dbo.Artist

UNION ALL

SELECT 'AlbumToArtist' AS [Table], COUNT(1) AS [Count]
FROM dbo.AlbumToArtist

UNION ALL

SELECT 'SongToArtist' AS [Table], COUNT(1) AS [Count]
FROM dbo.SongToArtist

UNION ALL

SELECT 'Platform' AS [Table], COUNT(1) AS [Count]
FROM dbo.Platform

UNION ALL

SELECT 'Reason' AS [Table], COUNT(1) AS [Count]
FROM dbo.Reason
UNION ALL

SELECT 'User' AS [Table], COUNT(1) AS [Count]
FROM dbo.[User]

UNION ALL

SELECT 'AudioFeature' AS [Table], COUNT(1) AS [Count]
FROM dbo.AudioFeature