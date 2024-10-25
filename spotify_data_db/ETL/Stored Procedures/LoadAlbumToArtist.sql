CREATE   PROCEDURE [ETL].[LoadAlbumToArtist]
    @json NVARCHAR(MAX)
AS
BEGIN
    DROP TABLE IF EXISTS #tmp
    DROP TABLE IF EXISTS #missing

    SELECT 
        JSON_VALUE(j.value, '$.album_uri') AS [album_uri], 
        JSON_VALUE(j.value, '$.artist_uri') AS [artist_uri]
    INTO #tmp
    FROM openjson(@json, '$.values') AS j

    SELECT t.album_uri, t.artist_uri
    INTO #missing
    FROM #tmp AS t
    LEFT JOIN dbo.Album AS album ON album.URI = t.album_uri
    LEFT JOIN dbo.Artist AS artist ON artist.URI = t.artist_uri
    WHERE album.ID IS NULL AND artist.ID IS NULL

    INSERT INTO dbo.Album(URI)
    SELECT m.album_uri
    FROM #missing AS m
    LEFT JOIN dbo.Album AS a ON a.URI = m.album_uri
    WHERE a.ID IS NULL

    INSERT INTO dbo.Artist(URI)
    SELECT m.artist_uri
    FROM #missing AS m
    LEFT JOIN dbo.Artist AS a ON a.URI = m.artist_uri
    WHERE a.ID IS NULL

    INSERT INTO dbo.AlbumToArtist(AlbumID, ArtistID)
    SELECT o.AlbumID, o.ArtistID
    FROM
    (
        SELECT album.ID AS AlbumID, artist.ID AS ArtistID
        FROM #tmp AS t
        JOIN dbo.Album AS album ON album.URI = t.album_uri
        JOIN dbo.Artist AS artist ON artist.URI = t.artist_uri

        EXCEPT

        SELECT AlbumID, ArtistID
        FROM dbo.AlbumToArtist        
    ) AS o

END