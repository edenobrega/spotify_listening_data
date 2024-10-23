using System.Data;
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
            currentAlbums = new ConcurrentDictionary<string, Album>(conn.Query<Album>("SELECT * FROM [dbo].[Album]").ToDictionary(k => k.URI, v => v));
            currentReasons = new ConcurrentDictionary<string, Reason>(conn.Query<Reason>("SELECT * FROM [dbo].[Reason]").ToDictionary(k => k.Name, v => v));
            currentSongs = new ConcurrentDictionary<string, Song>(conn.Query<Song>("SELECT * FROM [dbo].[Song]").ToDictionary(k => k.Track_Uri, v => v));
            currentPlatforms = new ConcurrentDictionary<string, Platform>(conn.Query<Platform>("SELECT * FROM [dbo].[Platform]").ToDictionary(k => k.Name, v => v));
            currentUsers = new ConcurrentDictionary<string, User>(conn.Query<User>("SELECT * FROM [dbo].[User]").ToDictionary(k => k.Name, v => v));
            conn.Close();

            logger.LogInformation("Finished getting current data");

            logger.LogInformation("Getting files from directory {0}", config.Directory);
            string[] files = Directory.GetFiles(config.Directory).Where(w => w.Contains(".json") && w.Contains("Audio")).ToArray();
            logger.LogInformation("{0} files found", files.Length);

            Parallel.ForEach(files, file =>
            {
                logger.LogInformation("{0} loading file", file);
                string fileData = File.ReadAllText(file);
                logger.LogInformation("{0} loaded", file);

                foreach (var item in JsonConvert.DeserializeObject<List<SongData>>(fileData) ?? [])
                {
                    if (item.spotify_episode_uri is null && item.master_metadata_track_name is not null)
                    {
                        allData.Add(item);
                    }
                }
            });


            var data_bulk = new List<SongData>();

            #region Reasons
            logger.LogInformation("Searching for new reasons");
            List<Reason> newReasons = new();

            List<Reason> existingReasons = data_bulk.Select(s => new Reason { Name = s.reason_start }).ToList();

            existingReasons.AddRange(data_bulk.Select(s => new Reason { Name = s.reason_end }).ToList());

            existingReasons = existingReasons.Distinct().ToList();

            foreach (var left in existingReasons)
            {
                if (!currentReasons.TryGetValue(left.Name, out _))
                {
                    newReasons.Add(left);
                }
            }

            if (newReasons.Count > 0)
            {
                logger.LogInformation("{0} new reasons found", newReasons.Count);
                inserted = influx.BulkInsert(newReasons);
                logger.LogInformation("{0} reasons inserted", inserted);
                conn.Open();
                currentReasons = conn.Query<Reason>("SELECT * FROM [dbo].[Reason]").ToDictionary(k => k.Name, v => v);
                conn.Close();
            }
            else
            {
                logger.LogInformation("No new reasons found");
            }
            #endregion

            #region Song
            logger.LogInformation("Searching for new songs");
            List<Song> newSongs = new();

            var xxxxx = data_bulk.Select(s => new Song { Name = s.master_metadata_track_name, Track_Uri = s.spotify_track_uri, AlbumName = s.master_metadata_album_album_name }).Distinct();

            foreach (var left in xxxxx)
            {
                if (!currentSongs.TryGetValue(left.Track_Uri, out _))
                {
                    Song newSong = new Song
                    {
                        Name = left.Name,
                        Track_Uri = left.Track_Uri
                    };
                    newSongs.Add(newSong);
                }
            }

            if (newSongs.Count() > 0)
            {
                logger.LogInformation("{0} new songs found", inserted);
                inserted = influx.BulkInsert(newSongs);
                logger.LogInformation("{0} songs inserted", inserted);
                conn.Open();
                currentSongs = conn.Query<Song>("SELECT * FROM [dbo].[Song]").ToDictionary(k => k.Track_Uri, v => v);
                conn.Close();
            }
            else
            {
                logger.LogInformation("No new songs found");
            }
            #endregion

            #region Platform
            logger.LogInformation("Searching for new platforms");
            List<Platform> newPlatforms = new();

            foreach (var left in data_bulk.Select(s => new Platform { Name = s.platform }).Distinct())
            {
                if (!currentPlatforms.TryGetValue(left.Name, out _))
                {
                    newPlatforms.Add(left);
                }
            }

            if (newPlatforms.Count > 0)
            {
                logger.LogInformation("{0} new platforms found", newPlatforms.Count);
                inserted = influx.BulkInsert(newPlatforms);
                logger.LogInformation("{0} platforms inserted", inserted);
                conn.Open();
                currentPlatforms = conn.Query<Platform>("SELECT * FROM [dbo].[Platform]").ToDictionary(k => k.Name, v => v);
                conn.Close();
            }
            else
            {
                logger.LogInformation("No new platforms found");
            }
            #endregion

            #region User
            logger.LogInformation("Searching for new user");
            List<User> newUsers = new();

            foreach (var left in data_bulk.Select(s => new User { Name = s.username }).Distinct())
            {
                if (!currentUsers.TryGetValue(left.Name, out _))
                {
                    newUsers.Add(left);
                }
            }

            if (newUsers.Count > 0)
            {
                logger.LogInformation("{0} new users inserted", newUsers.Count);
                inserted = influx.BulkInsert(newUsers);
                logger.LogInformation("{0} users inserted", inserted);
                conn.Open();
                currentUsers = conn.Query<User>("SELECT * FROM [dbo].[User]").ToDictionary(k => k.Name, v => v);
                conn.Close();
            }
            else
            {
                logger.LogInformation("No new users found");
            }
            #endregion

            #region Listening Data
            logger.LogInformation("Inserting listening data");
            List<Data> toAdd = new();

            foreach (var item in data_bulk)
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
            #endregion

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

        private static void LoadSongData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            InfluxSQL influx = new InfluxSQL(config.ConnectionString);

            List<Song> songs;
            using(var conn = new SqlConnection(config.ConnectionString))
            {
                songs = conn.Query<Song>("SELECT * FROM dbo.Song WHERE Album_URI IS NULL").ToList();
                logger.LogInformation("{0} songs without an album", songs.Count);
            }

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
                    batchCount = 0;
                    ids = ids.TrimEnd(',');
                    APIResponse apiData = GetApiData<APIResponse>("https://api.spotify.com/v1/tracks?ids="+ids, GetApiKey(config));
                    DateTime lastRequest = DateTime.Now;

                    responses.AddRange(apiData.tracks);

                    if ((lastRequest + TimeSpan.FromSeconds(2)) < DateTime.Now)
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(2));
                    }
                    
                    break;
                }
            }

            // .Select(s => new { song_uri = s.id, album_uri = s.album.id, artists = s.artists.Select(a => a.uri) })

            string json_updates = "";

            foreach (var item in responses)
            {
                foreach (var cross in item.artists)
                {
                    json_updates += JsonConvert.SerializeObject(new 
                    { 
                        song_uri = item.id,
                        album_uri = item.album.uri,
                        artist_uri = cross.uri
                    }) + ",";
                }
            }

            json_updates = json_updates.TrimEnd(',');

            json_updates = "{ \"values\":["+json_updates+"]}";

            List<Album> apiAlbums = responses.Select(s => new Album { URI = s.album.uri, Name = s.album.name }).Distinct().ToList();
            logger.LogInformation("{0} unique albums found from API", apiAlbums.Count);
            Dictionary<string, Album> currentAlbums;
            using (var conn = new SqlConnection(config.ConnectionString))
            {
                currentAlbums = conn.Query<Album>("SELECT [ID], [Name], [ArtistID], [URI] FROM [dbo].[Album]").ToDictionary(k => k.URI, v => v);
            }

            int inserted = -1;

            List<Album> newAlbums = new();
            foreach (var left in apiAlbums)
            {
                if (!currentAlbums.TryGetValue(left.URI, out _))
                {
                    newAlbums.Add(left);
                }
            }

            if (newAlbums.Count > 0)
            {
                logger.LogInformation("{0} new albums found", newAlbums.Count);
                inserted = influx.BulkInsert(newAlbums);
                logger.LogInformation("{0} new albums inserted", inserted);
            }

            //foreach (var item in responses)
            //{
            //    JsonConvert.SerializeObject(new { foo = "bar" })
            //}
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

                LoadStreamingData(logger, _config);
                //LoadSongData(logger, _config);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error has occurred : {ex.Message}");
                throw;
            }
        }
    }
}