using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SpotifyLoader.Models.API;
using SpotifyLoader.Models.Config;
using SpotifyLoader.Models;
using System.Collections.Concurrent;
using System.Data;
using Dapper;

namespace SpotifyLoader
{
    internal partial class Program
    {
        private static void LoadStreamingData(ILogger logger, Config config)
        {
            InfluxSQL influx = new InfluxSQL(config.ConnectionString);
            SqlConnection conn = new SqlConnection(config.ConnectionString);

            int inserted = -1;

            ConcurrentDictionary<string, Album> currentAlbums = new();
            ConcurrentDictionary<string, Reason> currentReasons = new();
            ConcurrentDictionary<string, Song> currentSongs = new();
            ConcurrentDictionary<string, Platform> currentPlatforms = new();
            ConcurrentDictionary<string, User> currentUsers = new();

            logger.LogInformation("Getting current data...");

            conn.Open();
            currentReasons = new ConcurrentDictionary<string, Reason>(conn.Query<Reason>("SELECT * FROM [dbo].[Reason]").ToDictionary(k => k.Name, v => v));
            currentSongs = new ConcurrentDictionary<string, Song>(conn.Query<Song>("SELECT * FROM [dbo].[Song]").ToDictionary(k => k.Track_Uri, v => v));
            currentPlatforms = new ConcurrentDictionary<string, Platform>(conn.Query<Platform>("SELECT * FROM [dbo].[Platform]").ToDictionary(k => k.Name, v => v));
            currentUsers = new ConcurrentDictionary<string, User>(conn.Query<User>("SELECT * FROM [dbo].[User]").ToDictionary(k => k.Name, v => v));
            conn.Close();

            logger.LogInformation("Finished getting current data");

            logger.LogInformation("Getting files from directory {0}", config.Directory);
            string[] files = Directory.GetFiles(config.Directory).Where(w => w.Contains(".json") && w.Contains("Audio")).ToArray();
            logger.LogInformation("{0} files found", files.Length);

            ConcurrentBag<Reason> newReasons = new ConcurrentBag<Reason>();
            ConcurrentBag<Song> newSongs = new ConcurrentBag<Song>();
            ConcurrentBag<Platform> newPlatforms = new ConcurrentBag<Platform>();
            ConcurrentBag<User> newUsers = new ConcurrentBag<User>();

            ConcurrentBag<SongData> ListeningData = new ConcurrentBag<SongData>();

            Parallel.ForEach(files, file =>
            {
                int fileID;
                using (SqlConnection conn = new SqlConnection(config.ConnectionString))
                {
                    var x = conn.QuerySingleOrDefault<bool>("SELECT TOP 1 1 FROM ETL.Files WHERE [Name] = @p1", new { p1 = file });
                    if (!x)
                    {
                        fileID = conn.QuerySingle<int>(StoredProcedures.AddFile, new { @name = file }, commandType: CommandType.StoredProcedure);
                    }
                    else
                    {
                        logger.LogInformation("{0} is already loaded", file);
                        return;
                    }
                }

                logger.LogInformation("{0} loading file", file);

                string fileData = File.ReadAllText(file);
                logger.LogInformation("{0} loaded", file);

                logger.LogInformation("{0} Searching for new data", file);
                foreach (var item in JsonConvert.DeserializeObject<List<SongData>>(fileData) ?? [])
                {
                    if (item.spotify_episode_uri is null && item.master_metadata_track_name is not null)
                    {
                        if (!currentReasons.TryGetValue(item.reason_start, out _))
                        {
                            Reason reasonStart = new Reason { Name = item.reason_start };
                            newReasons.Add(reasonStart);
                            currentReasons.TryAdd(item.reason_start, reasonStart);
                        }

                        if (item.reason_end is null)
                        {
                            item.reason_end = "UNSUPPORTED";
                        }

                        if (!currentReasons.TryGetValue(item.reason_end, out _))
                        {
                            Reason reasonEnd = new Reason { Name = item.reason_end };
                            newReasons.Add(reasonEnd);
                            currentReasons.TryAdd(item.reason_end, reasonEnd);
                        }

                        if (!currentSongs.TryGetValue(item.spotify_track_uri, out _))
                        {
                            Song song = new Song
                            {
                                Name = item.master_metadata_track_name,
                                Track_Uri = item.spotify_track_uri
                            };

                            newSongs.Add(song);
                            currentSongs.TryAdd(item.spotify_track_uri, song);
                        }

                        if (!currentPlatforms.TryGetValue(item.platform, out _))
                        {
                            Platform platform = new Platform
                            {
                                Name = item.platform
                            };
                            newPlatforms.Add(platform);
                            currentPlatforms.TryAdd(item.platform, platform);
                        }

                        if (!currentUsers.TryGetValue(item.username, out _))
                        {
                            User user = new User { Name = item.username };
                            newUsers.Add(user);
                            currentUsers.TryAdd(item.username, user);
                        }

                        item.FileID = fileID;

                        ListeningData.Add(item);
                    }
                }
                logger.LogInformation("{0} finished searching file for new data", file);
            });

            logger.LogInformation("Searching for data finished");

            if (newReasons.Count > 0)
            {
                influx.BulkInsert(newReasons);
            }
            if (newSongs.Count > 0)
            {
                influx.BulkInsert(newSongs);
            }
            if (newPlatforms.Count > 0)
            {
                influx.BulkInsert(newPlatforms);
            }
            if (newUsers.Count > 0)
            {
                influx.BulkInsert(newUsers);
            }

            newReasons.Clear();
            newSongs.Clear();
            newPlatforms.Clear();
            newUsers.Clear();

            logger.LogInformation("Refreshing data . . .");

            conn.Open();
            currentReasons = new ConcurrentDictionary<string, Reason>(conn.Query<Reason>("SELECT * FROM [dbo].[Reason]").ToDictionary(k => k.Name, v => v));
            currentSongs = new ConcurrentDictionary<string, Song>(conn.Query<Song>("SELECT * FROM [dbo].[Song]").ToDictionary(k => k.Track_Uri, v => v));
            currentPlatforms = new ConcurrentDictionary<string, Platform>(conn.Query<Platform>("SELECT * FROM [dbo].[Platform]").ToDictionary(k => k.Name, v => v));
            currentUsers = new ConcurrentDictionary<string, User>(conn.Query<User>("SELECT * FROM [dbo].[User]").ToDictionary(k => k.Name, v => v));
            conn.Close();

            // TODO: Should probably attempt to now insert duplicate listen data
            if (ListeningData.IsEmpty)
            {
                logger.LogInformation("No data to load");
                return;
            }
            logger.LogInformation("Inserting listening data");
            List<Data> toAdd = new();

            foreach (var item in ListeningData)
            {
                DateTime? date = null;
                if (item.offline_timestamp is not null)
                {
                    DateTime start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    date = start.AddMilliseconds((double)item.offline_timestamp).ToUniversalTime();
                }

                toAdd.Add(new Data
                {
                    FileID = item.FileID,

                    OfflineTimestamp = date,
                    EndTime = item.ts,
                    Country = item.conn_country,
                    Skipped = item.skipped,
                    Offline = item.offline,
                    IncognitoMode = item.incognito_mode,
                    Shuffle = item.shuffle,
                    ms_played = item.ms_played,

                    SongID = currentSongs[item.spotify_track_uri].ID,
                    ReasonStart = currentReasons[item.reason_start].ID,
                    ReasonEnd = currentReasons[item.reason_end].ID,
                    PlatformID = currentPlatforms[item.platform].ID,
                    UserID = currentUsers[item.username].ID
                });

                if (config.BulkSize == toAdd.Count)
                {
                    logger.LogInformation("inserting next {0} rows of listening data", config.BulkSize);
                    inserted = influx.BulkInsert(toAdd);
                    logger.LogInformation("{0} rows successfuly inserted", inserted);
                    toAdd.Clear();
                }
            }

            if (toAdd.Count > 0)
            {
                logger.LogInformation("inserting final {0} rows of listening data", toAdd.Count);
                inserted = influx.BulkInsert(toAdd);
                logger.LogInformation("{0} rows successfuly inserted", inserted);
                toAdd.Clear();
            }
        }

        private static void LoadAudioFeatureData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            logger.LogInformation("Starting LoadAudioFeatureData");
            InfluxSQL influx = new InfluxSQL(config.ConnectionString);

            logger.LogInformation("Loading Audio Feature File");
            string fileData = File.ReadAllText(config.SongAlbumDirectory + "\\" + "SongAudioFeatures.json");
            var tmp = JsonConvert.DeserializeObject<FeatureItem[]>(fileData).Where(w => w is not null).ToArray();
            if (tmp is null || tmp.Length == 0)
            {
                logger.LogInformation("No audio feature data found");
                return;
            }



            var responses = tmp.ToDictionary(k => "spotify:track:"+k.ID, v => v);

            Dictionary<string, int> newFeatures;
            using (var conn = new SqlConnection(config.ConnectionString))
            {
                string sql = @"SELECT [ID], [Track_Uri]
                                FROM [dbo].[Song] AS s 
                                LEFT JOIN [dbo].[AudioFeature] AS af ON af.SongID = s.ID
                                WHERE af.SongID IS NULL";
                newFeatures = conn.Query(sql).ToDictionary(k => (string)k.Track_Uri, v => (int)v.ID);
            }

            List<FeatureItem> bulkAdd = new List<FeatureItem>();

            foreach (var feature in newFeatures)
            {
                if (responses.TryGetValue(feature.Key, out FeatureItem value))
                {
                    value.SongID = feature.Value;
                    bulkAdd.Add(value);
                }

                if(bulkAdd.Count == config.BulkSize)
                {
                    int inserted = influx.BulkInsert(bulkAdd);
                    logger.LogInformation("{0} Audio features inserted", inserted);
                    bulkAdd.Clear();
                }
            }

            if (bulkAdd.Count > 0)
            {
                int inserted = influx.BulkInsert(bulkAdd);
                logger.LogInformation("{0} Audio features inserted", inserted);
            }
        }

        private static void LoadSongData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            logger.LogInformation("LoadSongData Start");

            InfluxSQL influx = new InfluxSQL(config.ConnectionString);
            int inserted = -1;


            Dictionary<string, Album> currentAlbums;
            Dictionary<string, Artist> currentArtists;

            SqlConnection conn = new SqlConnection(config.ConnectionString);

            logger.LogInformation("Getting existing, song, album, artists");

            conn.Open();
            currentAlbums = conn.Query<Album>("SELECT * FROM dbo.Album").ToDictionary(k => k.URI, v => v);
            currentArtists = conn.Query<Artist>("SELECT * FROM dbo.Artist").ToDictionary(k => k.URI, v => v);
            conn.Close();


            string fileData = File.ReadAllText(config.SongAlbumDirectory + "\\" + config.SongAlbumFileName);

            var responses = JsonConvert.DeserializeObject<List<TrackResponse>>(fileData) ?? [];

            // insert new albums
            logger.LogInformation("Finding new albums");
            List<Album> newAlbums = new List<Album>();
            foreach (var item in responses)
            {
                if (!currentAlbums.TryGetValue(item.album.uri, out _))
                {
                    Album newAlbum = new Album { Name = item.album.name, URI = item.album.uri };
                    newAlbums.Add(newAlbum);
                    currentAlbums.Add(item.album.uri, newAlbum);
                }
            }

            if (newAlbums.Count > 0)
            {
                logger.LogInformation("{0} new albums found", newAlbums.Count);
                inserted = influx.BulkInsert(newAlbums);
                logger.LogInformation("{0} new albums inserted", inserted);
            }
            else
            {
                logger.LogInformation("No new albums found");
            }

            // insert new artists
            logger.LogInformation("Finding new artists");
            List<Artist> newArtists = new List<Artist>();
            foreach (var response in responses)
            {
                foreach (var artist in response.artists)
                {
                    if (!currentArtists.TryGetValue(artist.uri, out _))
                    {
                        Artist newArtist = new Artist { Name = artist.name, URI = artist.uri };
                        newArtists.Add(newArtist);
                        currentArtists.Add(artist.uri, newArtist);
                    }
                }
            }

            if (newArtists.Count > 0)
            {
                logger.LogInformation("{0} new artists found", newArtists.Count);
                inserted = influx.BulkInsert(newArtists);
                logger.LogInformation("{0} new artists inserted", inserted);
            }
            else
            {
                logger.LogInformation("No new artists found");
            }

            logger.LogInformation("Updating AlbumToArtist table and AlbumID column in Song table");

            string json_album_to_artist = "";
            string json_song_to_album = "";
            string json_song_to_artist = "";

            int batchCountSong = 0;
            int batchCountArtist = 0;
            int batchCountSongToArtist = 0;

            // TODO: so much repeated code, this can 100% be improved, and should be

            // album to artist update
            foreach (var item in responses)
            {
                json_song_to_album += JsonConvert.SerializeObject(new
                {
                    song_uri = item.uri,
                    album_uri = item.album.uri,
                    item.duration_ms
                }) + ",";
                batchCountSong++;
                if (batchCountSong == 100)
                {
                    logger.LogInformation("Batch count reached, Updating Song table");
                    json_song_to_album = json_song_to_album.TrimEnd(',');
                    json_song_to_album = "{ \"values\":[" + json_song_to_album + "]}";
                    conn.Open();
                    conn.Execute("[ETL].[LoadSongToAlbum]", new { json = json_song_to_album }, commandType: CommandType.StoredProcedure);
                    conn.Close();
                    json_song_to_album = string.Empty;
                    batchCountSong = 0;
                    logger.LogInformation("Song table updated");
                }

                for (int i = 0; i < item.album.artists.Length; i++)
                {
                    var cross = item.album.artists[i];
                    json_album_to_artist += JsonConvert.SerializeObject(new
                    {
                        primary = i == 0 ? 1 : 0,
                        album_uri = item.album.uri,
                        artist_uri = cross.uri
                    }) + ",";
                    batchCountArtist++;
                    if (batchCountArtist == 100)
                    {
                        logger.LogInformation("Batch count reached, Updating AlbumToArtist table");

                        json_album_to_artist = json_album_to_artist.TrimEnd(',');
                        json_album_to_artist = "{ \"values\":[" + json_album_to_artist + "]}";

                        conn.Open();
                        conn.Execute("[ETL].[LoadAlbumToArtist]", new { json = json_album_to_artist }, commandType: CommandType.StoredProcedure);
                        conn.Close();
                        json_album_to_artist = string.Empty;
                        batchCountArtist = 0;
                        logger.LogInformation("AlbumToArtist table updated");
                    }
                }

                for (int i = 0; i < item.artists.Length; i++)
                {
                    var cross_b = item.artists[i];

                    json_song_to_artist += JsonConvert.SerializeObject(new
                    {
                        primary = i == 0 ? 1 : 0,
                        song_uri = item.uri,
                        artist_uri = item.artists[i].uri
                    }) + ",";

                    batchCountSongToArtist++;
                    if (batchCountSongToArtist == 100)
                    {
                        logger.LogInformation("Batch count reached, Updating SongToArtist table");

                        json_song_to_artist = json_song_to_artist.TrimEnd(',');
                        json_song_to_artist = "{ \"values\":[" + json_song_to_artist + "]}";

                        conn.Open();
                        conn.Execute("[ETL].[LoadSongToArtist]", new { json = json_song_to_artist }, commandType: CommandType.StoredProcedure);
                        conn.Close();
                        json_song_to_artist = string.Empty;
                        batchCountSongToArtist = 0;
                        logger.LogInformation("SongToArtist table updated");
                    }
                }
            }

            if (batchCountArtist > 0)
            {
                logger.LogInformation("Final AlbumToArtist table update");

                json_album_to_artist = json_album_to_artist.TrimEnd(',');
                json_album_to_artist = "{ \"values\":[" + json_album_to_artist + "]}";

                conn.Open();
                conn.Execute("[ETL].[LoadAlbumToArtist]", new { json = json_album_to_artist }, commandType: CommandType.StoredProcedure);
                conn.Close();

                logger.LogInformation("AlbumToArtist table updated");
            }

            if (batchCountSong > 0)
            {
                logger.LogInformation("Final Song table updates");

                json_song_to_album = json_song_to_album.TrimEnd(',');
                json_song_to_album = "{ \"values\":[" + json_song_to_album + "]}";

                conn.Open();
                conn.Execute("[ETL].[LoadSongToAlbum]", new { json = json_song_to_album }, commandType: CommandType.StoredProcedure);
                conn.Close();

                logger.LogInformation("Song table updated");
            }

            if (batchCountSongToArtist > 0)
            {
                logger.LogInformation("Batch count reached, Updating SongToArtist table");

                json_song_to_artist = json_song_to_artist.TrimEnd(',');
                json_song_to_artist = "{ \"values\":[" + json_song_to_artist + "]}";

                conn.Open();
                conn.Execute("[ETL].[LoadSongToArtist]", new { json = json_song_to_artist }, commandType: CommandType.StoredProcedure);
                conn.Close();
                json_song_to_artist = string.Empty;
                batchCountSongToArtist = 0;
                logger.LogInformation("SongToArtist table updated");
            }

            logger.LogInformation("LoadSongData finished running");
        }
    }
}
