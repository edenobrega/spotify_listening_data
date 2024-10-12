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

        private static DataTable CreateDataTable()
        {
            DataTable _dataTable = new DataTable();
            _dataTable.Columns.Add(new DataColumn("end_time", typeof(DateTime)));
            _dataTable.Columns.Add(new DataColumn("ms_played", typeof(int)));
            _dataTable.Columns.Add(new DataColumn("reason_start", typeof(int)));
            _dataTable.Columns.Add(new DataColumn("reason_end", typeof(int)));
            _dataTable.Columns.Add(new DataColumn("platform_id", typeof(int)));
            _dataTable.Columns.Add(new DataColumn("song_id", typeof(int)));
            _dataTable.Columns.Add(new DataColumn("country", typeof(string)));
            _dataTable.Columns.Add(new DataColumn("skipped", typeof(bool)));
            _dataTable.Columns.Add(new DataColumn("offline", typeof(bool)));
            _dataTable.Columns.Add(new DataColumn("offline_timestamp", typeof(double)));
            _dataTable.Columns.Add(new DataColumn("incognito_mode", typeof(bool)));
            _dataTable.Columns.Add(new DataColumn("shuffle", typeof(bool)));
            _dataTable.Columns.Add(new DataColumn("user_id", typeof(int)));
            return _dataTable;
        }

        private static void LoadStreamingData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            InfluxSQL influx = new InfluxSQL(config.ConnectionString);
            SqlConnection conn = new SqlConnection(config.ConnectionString);

            int inserted = -1;

            Dictionary<string, Artist> currentArtists = new();
            ILookup<string, Album> currentAlbums;
            Dictionary<string, Reason> currentReasons = new();
            Dictionary<string, Song> currentSongs = new();
            Dictionary<string, Platform> currentPlatforms = new();
            Dictionary<string, User> currentUsers = new();

            logger.LogInformation("Getting current data...");

            conn.Open();
            currentAlbums = conn.Query<Album>(StoredProcedures.GetAlbums, commandType: CommandType.StoredProcedure).ToLookup(k => k.Name, v => v);
            currentArtists = conn.Query<Artist>("SELECT * FROM [dbo].[Artist]").ToDictionary(k => k.Name, v => v);
            currentReasons = conn.Query<Reason>("SELECT * FROM [dbo].[Reason]").ToDictionary(k => k.Name, v => v);
            currentSongs = conn.Query<Song>("SELECT * FROM [dbo].[Song]").ToDictionary(k => k.Name, v => v);
            currentPlatforms = conn.Query<Platform>("SELECT * FROM [dbo].[Platform]").ToDictionary(k => k.Name, v => v);
            currentUsers = conn.Query<User>("SELECT * FROM [dbo].[User]").ToDictionary(k => k.Name, v => v);
            conn.Close();

            logger.LogInformation("Finished getting current data");

            List<SongData> data_bulk;

            logger.LogInformation("Getting files from directory {0}", config.Directory);
            string[] files = Directory.GetFiles(config.Directory).Where(w => w.Contains(".json") && w.Contains("Audio")).ToArray();
            logger.LogInformation("{0} files found", files.Length);
            foreach (string file in files)
            {
                logger.LogInformation("Loading file {0}", file);

                string fileData = File.ReadAllText(file);

                if (fileData is null)
                {
                    logger.LogWarning("No data found in file {0}", file);
                    continue;
                }

                data_bulk = JsonConvert.DeserializeObject<List<SongData>>(fileData) ?? [];

                if (data_bulk.Count == 0)
                {
                    logger.LogWarning("Converting file {0} to object list failed", file);
                    continue;
                }

                #region Artists
                logger.LogInformation("Searching for new artists");
                List<Artist> newArtists = new();

                foreach (var left in data_bulk.Select(s => new Artist { Name = s.master_metadata_album_artist_name }).Distinct().ToList())
                {
                    if (!currentArtists.TryGetValue(left.Name, out _))
                    {
                        newArtists.Add(left);
                    }
                }
                logger.LogInformation("{0} new artists found", newArtists.Count);

                if (newArtists.Count != 0)
                {
                    inserted = influx.BulkInsert(newArtists);
                    logger.LogInformation("{0} artists inserted", inserted);
                    conn.Open();
                    currentArtists = conn.Query<Artist>("SELECT * FROM [dbo].[Artist]").ToDictionary(k => k.Name, v => v);
                    conn.Close();
                }
                #endregion

                #region Album
                logger.LogInformation("Searching for new albums");
                List<Album> newAlbums = new();

                foreach (var left in data_bulk.Select(s => new Album { Name = s.master_metadata_album_album_name, ArtistID = currentArtists[s.master_metadata_album_artist_name].ID, ArtistName =  s.master_metadata_album_artist_name }).Distinct().ToList())
                {
                    var x = currentAlbums[left.Name];
                    if (!x.Any())
                    {
                        Album _album;
                        switch (x.Count())
                        {
                            case 0:
                                break;
                            case 1:
                                _album = x.First();
                                break;
                            default:
                                _album = x.Single(s => s.ArtistName == left.ArtistName);
                                break;
                        }

                        newAlbums.Add(left);
                    }
                }
                logger.LogInformation("{0} new albums found", newArtists.Count);

                if (newAlbums.Count != 0)
                {
                    inserted = influx.BulkInsert(newAlbums);
                    logger.LogInformation("{0} albums inserted", inserted);
                    conn.Open();
                    currentAlbums = conn.Query<Album>(StoredProcedures.GetAlbums, commandType: CommandType.StoredProcedure).ToLookup(k => k.Name, v => v);
                    conn.Close();
                }
                #endregion

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

                if (newReasons.Count != 0)
                {
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
                #endregion

                break;
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
                    FeaturesResponse? features = JsonConvert.DeserializeObject<FeaturesResponse>(response.Content);
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

        private static void Main()
        {
            var seriLogger = new LoggerConfiguration()
                .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Verbose)
                .CreateLogger();

            var logger = new SerilogLoggerFactory(seriLogger).CreateLogger("Loader");

            try
            {
                Config? _config = JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json"));
                if (_config is null)
                {
                    throw new Exception("Config loaded as null");
                }

                LoadStreamingData(logger, _config);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error has occurred : {ex.Message}");
                throw;
            }

            //LoadStreamingData();
            //LoadAudioFeatureData();
        }
    }
}