

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SpotifyLoader.Models.API;
using SpotifyLoader.Models.Config;
using SpotifyLoader.Models;
using Dapper;

namespace SpotifyLoader
{
    internal partial class Program
    {
        private static void DownloadAudioFeaturesData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            string[] songs;
            using (SqlConnection conn = new SqlConnection(config.ConnectionString))
            {
                string sql = @"SELECT [Track_Uri]
									FROM [dbo].[Song]
									where id not in (SELECT [SongID]
									FROM [dbo].[AudioFeature]) and Track_Uri is not null";
                songs = conn.Query<string>(sql).ToArray();
            }

            List<FeatureItem> responses = new List<FeatureItem>();

            DateTime lastRequest = DateTime.Now;
            int loopCount = (int)Math.Ceiling(songs.Length / 100f);
            for (int i = 0; i < loopCount; i++)
            {
                Thread.Sleep(TimeSpan.FromSeconds(0.5));
                responses.AddRange(GetApiData<FeatureResponse>("https://api.spotify.com/v1/audio-features?ids=" + string.Join(",", songs.Skip(i * 100).Take(100).Select(s => s.Replace("spotify:track:", ""))).TrimEnd(','), GetApiKey(config)).audio_features);
            }

            string filePath = config.SongAlbumDirectory + "\\SongAudioFeatures.json";

            if (File.Exists(filePath))
            {
                File.Move(filePath, config.SongAlbumDirectory + "\\" + "old_file.json");
            }

            File.WriteAllText(filePath, JsonConvert.SerializeObject(responses));

            if (File.Exists(config.SongAlbumDirectory + "\\" + "old_file_features.json"))
            {
                File.Delete(config.SongAlbumDirectory + "\\" + "old_file_features.json");
            }
        }

        private static void DownloadSongAlbumData(Microsoft.Extensions.Logging.ILogger logger, Config config)
        {
            logger.LogInformation("DownloadSongAlbumData Start");

            string filePath = config.SongAlbumDirectory + "\\" + config.SongAlbumFileName;

            List<Song> songs;
            using var conn = new SqlConnection(config.ConnectionString);
            songs = conn.Query<Song>("SELECT * FROM dbo.Song WHERE AlbumID IS NULL").ToList();
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
                    APIResponse apiData = GetApiData<APIResponse>("https://api.spotify.com/v1/tracks?ids=" + ids, GetApiKey(config));
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
            logger.LogInformation("Saving to file {0} in location {1}", config.SongAlbumDirectory, config.SongAlbumFileName);

            if (File.Exists(filePath))
            {
                File.Move(filePath, config.SongAlbumDirectory + "\\" + "old_file.json");
            }

            File.WriteAllText(filePath, JsonConvert.SerializeObject(responses));

            if (File.Exists(config.SongAlbumDirectory + "\\" + "old_file.json"))
            {
                File.Delete(config.SongAlbumDirectory + "\\" + "old_file.json");
            }
        }
    }
}
