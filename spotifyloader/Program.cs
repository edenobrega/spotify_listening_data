﻿using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using RestSharp;
using Serilog.Events;
using Serilog.Extensions.Logging;
using Serilog;
using SpotifyLoader.Models;
using SpotifyLoader.Models.API;
using Microsoft.Extensions.Logging;
using SpotifyLoader.Models.Config;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting conclassor. Consider declaring as nullable.

namespace SpotifyLoader
{
    internal class Program
    {
        private static DateTime _apiKeyAge;
        private static AuthResponse _currentApiAuth;

        private static string GetApiKey(Config config)
        {
            if (_currentApiAuth is null || (_apiKeyAge + TimeSpan.FromSeconds(_currentApiAuth.ExpiresIn)) < DateTime.Now)
            {
                const string url = "https://accounts.spotify.com/api/token";
                Uri uri = new Uri(url);
                RestClientOptions options = new RestClientOptions($"{uri.Scheme}://{uri.Authority}");
                RestClient client = new RestClient(options);
                client.AddDefaultParameter("grant_type", "client_credentials");
                client.AddDefaultParameter("client_id", config.ClientID);
                client.AddDefaultParameter("client_secret", config.ClientSecret);
                RestRequest request = new RestRequest(uri.AbsoluteUri);
                RestResponse response = client.Post(request);
                if (response.Content == null)
                {
                    throw new Exception("Response was empty.");
                }
                _currentApiAuth = JsonConvert.DeserializeObject<AuthResponse>(response.Content) ?? throw new InvalidOperationException();
                _apiKeyAge = DateTime.Now;
            }
            return _currentApiAuth.AccessToken;
        }

        private static T GetApiData<T>(string url, string bearer)
        {
            Uri uri = new Uri(url);
            RestClientOptions options = new RestClientOptions($"{uri.Scheme}://{uri.Authority}");
            RestClient client = new RestClient(options);
            client.AddDefaultHeader("Authorization", $"Bearer {bearer}");
            RestRequest request = new RestRequest(uri.AbsoluteUri);
            RestResponse response = client.Get(request);

            if (response.Content is null)
            {
                throw new Exception("Something went wrong");
            }

            return JsonConvert.DeserializeObject<T>(response.Content);
        }

        private static void LoadStreamingData(Microsoft.Extensions.Logging.ILogger logger, Config config)
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

                        ListeningData.Add(item);
                    }
                }
                logger.LogInformation("{0} finished searching file for new data", file);
            });

            logger.LogInformation("Searching for data finished");

            influx.BulkInsert(newReasons);
            influx.BulkInsert(newSongs);
            influx.BulkInsert(newPlatforms);
            influx.BulkInsert(newUsers);

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

        private static void LoadAudioFeatureData(Config config)
        {
            using (SqlConnection conn = new SqlConnection(config.ConnectionString))
            {
                void InsertData(Dictionary<string, int> kvps)
                {
                    string ids = "";
                    foreach (KeyValuePair<string, int> kvp in kvps)
                    {
                        ids += kvp.Key + ",";
                    }
                    ids = ids.Remove(ids.Length - 1);
                    string url = "https://api.spotify.com/v1/audio-features?ids=" + ids;
                    Uri uri = new Uri(url);
                    RestClientOptions options = new RestClientOptions($"{uri.Scheme}://{uri.Authority}");
                    RestClient client = new RestClient(options);
                    client.AddDefaultHeader("Authorization", $"Bearer {GetApiKey(config)}");
                    RestRequest request = new RestRequest(uri.AbsoluteUri);
                    RestResponse response = client.Get(request);
                    if (response.Content is null)
                    {
                        // todo: proper logging
                        return;
                    }
                    APIResponse? features = JsonConvert.DeserializeObject<APIResponse>(response.Content);
                    string insertSql = @"insert into dbo.AudioFeature([SongID]
																,[Duration]
																,[Acousticness]
																,[Danceability]
																,[Energy]
																,[Instrumentalness]
																,[Key]
																,[Liveness]
																,[Loudness]
																,[Mode]
																,[Speechiness]
																,[Tempo]
																,[TimeSignature]
																,[Valence])
										values(@SongID, @Duration, @Acousticness, @Danceability, @Energy, @Instrumentalness, @Key, @Liveness, @Loudness, @Mode, @Speechiness, @Tempo, @TimeSignature, @Valence)";

                    if (features is null)
                    {
                        // todo: logging
                        return;
                    }

                    foreach (FeatureResponse? feature in features.audio_features)
                    {
                        if (feature is null)
                        {
                            continue;
                        }
                        conn.Execute(insertSql, new
                        {
                            SongID = kvps[feature.ID],
                            feature.Duration,
                            feature.Acousticness,
                            feature.Danceability,
                            feature.Energy,
                            feature.Instrumentalness,
                            feature.Key,
                            feature.Liveness,
                            feature.Loudness,
                            feature.Mode,
                            feature.Speechiness,
                            feature.Tempo,
                            feature.TimeSignature,
                            feature.Valence
                        });
                    }
                }

                Dictionary<string, int> songs;
                string sql = @"SELECT [ID]
									,[Track_Uri]
									FROM [dbo].[Song]
									where id not in (SELECT [SongID]
									FROM [dbo].[AudioFeature]) and Track_Uri is not null";
                songs = conn.Query<Song>(sql).ToDictionary(k => k.Track_Uri, v => v.ID);

                DateTime lastRequest = DateTime.Now;
                int loopCount = (int)Math.Ceiling(songs.Count / 100f);
                for (int i = 0; i < loopCount; i++)
                {
                    if ((lastRequest + TimeSpan.FromSeconds(2)) < DateTime.Now)
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(2));
                    }
                    InsertData(songs.Skip(i * 100).Take(100).ToDictionary(k => k.Key.Replace("spotify:track:", ""), v => v.Value));
                }
            }
        }

        // Albums without linked artists (yet)
        //SELECT a.ID, a.Name, a.URI
        //FROM dbo.album as a
        //LEFT JOIN dbo.AlbumToArtist AS ata ON ata.AlbumID = a.ID
        //GROUP BY a.ID, a.Name, a.URI
        //HAVING COUNT(ata.AlbumID) = 0

        private static void LoadSongData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            logger.LogInformation("LoadSongData Start");

            InfluxSQL influx = new InfluxSQL(config.ConnectionString);
            int inserted = -1;

            List<Song> songs;
            Dictionary<string, Album> currentAlbums;
            Dictionary<string, Artist> currentArtists;

            SqlConnection conn = new SqlConnection(config.ConnectionString);

            logger.LogInformation("Getting existing, song, album, artists");

            conn.Open();
            songs = conn.Query<Song>("SELECT * FROM dbo.Song WHERE AlbumID IS NULL").ToList();
            currentAlbums = conn.Query<Album>("SELECT * FROM dbo.Album").ToDictionary(k => k.URI, v => v);
            currentArtists = conn.Query<Artist>("SELECT * FROM dbo.Artist").ToDictionary(k => k.URI, v => v);
            conn.Close();
            
            logger.LogInformation("{0} songs without an album", songs.Count);

            logger.LogInformation("Getting song data from API");

            List<TrackResponse> responses = new List<TrackResponse>();
            int batchCount = 0;
            string ids = "";
            foreach (var song in songs)
            {
                ids += song.Track_Uri.Replace("spotify:track:", string.Empty) + ",";
                batchCount++;
                if (batchCount == 50)
                {
                    logger.LogInformation("Batch count reached, requesting from API");
                    batchCount = 0;
                    ids = ids.TrimEnd(',');
                    APIResponse apiData = GetApiData<APIResponse>("https://api.spotify.com/v1/tracks?ids="+ids, GetApiKey(config));
                    DateTime lastRequest = DateTime.Now;

                    responses.AddRange(apiData.tracks);

                    ids = string.Empty;

                    Thread.Sleep(TimeSpan.FromSeconds(0.5));
                }
            }
            if (batchCount != 0)
            {
                logger.LogInformation("Requesting final API values");
                ids = ids.TrimEnd(',');
                APIResponse apiData = GetApiData<APIResponse>("https://api.spotify.com/v1/tracks?ids=" + ids, GetApiKey(config));
                DateTime lastRequest = DateTime.Now;

                responses.AddRange(apiData.tracks);
            }

            logger.LogInformation("Finished getting data from API");

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

            int batchCountSong = 0;
            int batchCountArtist = 0;

            // album to artist update
            foreach (var item in responses)
            {
                json_song_to_album += JsonConvert.SerializeObject(new
                {
                    song_uri = "spotify:track:"+item.id,
                    album_uri = item.album.uri
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
                foreach (var cross in item.artists)
                {
                    json_album_to_artist += JsonConvert.SerializeObject(new 
                    { 
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

            logger.LogInformation("LoadSongData finished running");
        }
        private static void Main()
        {
            var seriLogger = new LoggerConfiguration()
                .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Verbose)
                .CreateLogger();

            var logger = new SerilogLoggerFactory(seriLogger).CreateLogger("Loader");

            try
            {
                Config? _config = JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json")) ?? throw new Exception("Config loaded as null");

                //LoadStreamingData(logger, _config);
                LoadSongData(logger, _config);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error has occurred : {ex.Message}");
                throw;
            }
        }
    }
}