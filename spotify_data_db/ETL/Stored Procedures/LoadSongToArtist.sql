CREATE   PROCEDURE [ETL].[LoadSongToArtist]
    @json NVARCHAR(MAX)
AS
BEGIN
    DROP TABLE IF EXISTS #tmp
    DROP TABLE IF EXISTS #missing

    SELECT 
        JSON_VALUE(j.value, '$.primary') AS [primary], 
        JSON_VALUE(j.value, '$.song_uri') AS [song_uri], 
        JSON_VALUE(j.value, '$.artist_uri') AS [artist_uri]
    INTO #tmp
    FROM openjson(@json, '$.values') AS j

    SELECT t.artist_uri, t.song_uri
    INTO #missing
    FROM #tmp AS t
    LEFT JOIN dbo.Artist AS album ON album.URI = t.artist_uri
    WHERE album.ID IS NULL

    INSERT INTO dbo.Artist(URI)
    SELECT m.artist_uri
    FROM #missing AS m
    LEFT JOIN dbo.Artist AS a ON a.URI = m.artist_uri
    WHERE a.ID IS NULL

    INSERT INTO dbo.SongToArtist([Primary], SongID, ArtistID)
    SELECT [primary], o.SongID, o.ArtistID
    FROM
    (
        SELECT t.[primary], song.ID AS SongID, artist.ID AS ArtistID
        FROM #tmp AS t
        JOIN dbo.Song AS song ON song.Track_Uri = t.song_uri
        JOIN dbo.Artist AS artist ON artist.URI = t.artist_uri

        EXCEPT

        SELECT [Primary] ,SongID, ArtistID
        FROM dbo.SongToArtist        
    ) AS o
END