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
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

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

        private static void LoadStreamingDataNew(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            SqlConnection conn = new SqlConnection(config.ConnectionString);

            List<Artist> artists = new();
            List<Album> albums = new();
            List<Reason> reasons = new();
            List<Song> songs = new();
            List<Platform> platforms = new();
            List<User> users = new();

            logger.LogInformation("Getting current data...");

            string sql = string.Empty;
            conn.Open();
            sql = "SELECT * FROM [dbo].[Album]";
            albums = conn.Query<Album>(sql).ToList();
            sql = "SELECT * FROM [dbo].[Artist]";
            artists = conn.Query<Artist>(sql).ToList();
            sql = "SELECT * FROM [dbo].[Reason]";
            reasons = conn.Query<Reason>(sql).ToList();
            sql = "SELECT * FROM [dbo].[Song]";
            songs = conn.Query<Song>(sql).ToList();
            sql = "SELECT * FROM [dbo].[Platform]";
            platforms = conn.Query<Platform>(sql).ToList();
            sql = "SELECT * FROM [dbo].[User]";
            users = conn.Query<User>(sql).ToList();
            conn.Close();

            logger.LogInformation("Finished getting current data");

            List<SongData> data_bulk;

            logger.LogInformation("Getting files from directory {}", config.Directory);
            string[] files = Directory.GetFiles(config.Directory).Where(w => w.Contains(".json") && w.Contains("Audio")).ToArray();
            logger.LogInformation("{} files found", config.Directory.Length);
            foreach (string file in files)
            {
                logger.LogInformation("Loading file {}", file);

                string fileData = File.ReadAllText(file);

                if (fileData is null)
                {
                    logger.LogWarning("No data found in file {}", file);
                    continue;
                }

                data_bulk = JsonConvert.DeserializeObject<List<SongData>>(fileData) ?? new List<SongData>();

                if (data_bulk.Count == 0)
                {
                    logger.LogWarning("Converting file {} to object list failed", file);
                    continue;
                }



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
                LoadStreamingDataNew(logger, _config);
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