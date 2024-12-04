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
    internal partial class Program
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
                //DownloadSongAlbumData(logger, _config);
                //LoadSongData(logger, _config);
                DownloadAudioFeaturesData(logger, _config);
                LoadAudioFeatureData(logger, _config);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error has occurred : {ex.Message}");
                throw;
            }
        }
    }
}