CREATE   PROCEDURE ETL.LoadSongToAlbum
    @json NVARCHAR(MAX)
AS
BEGIN
    DROP TABLE IF EXISTS #tmp
    DROP TABLE IF EXISTS #missing

    SELECT 
        JSON_VALUE(j.value, '$.song_uri') AS [song_uri], 
        JSON_VALUE(j.value, '$.album_uri') AS [album_uri]
    INTO #tmp
    FROM openjson(@json, '$.values') AS j

    SELECT t.album_uri, t.song_uri
    INTO #missing
    FROM #tmp AS t
    LEFT JOIN dbo.Album AS album ON album.URI = t.album_uri
    WHERE album.ID IS NULL

    INSERT INTO dbo.Album(URI)
    SELECT m.album_uri
    FROM #missing AS m
    LEFT JOIN dbo.Album AS a ON a.URI = m.album_uri
    WHERE a.ID IS NULL

    UPDATE s
    SET AlbumID = a.ID
    FROM dbo.Song AS s
    JOIN #tmp AS t ON t.song_uri = s.Track_Uri
    JOIN dbo.Album AS a ON a.URI = t.album_uri
    WHERE s.AlbumID IS NULL
END