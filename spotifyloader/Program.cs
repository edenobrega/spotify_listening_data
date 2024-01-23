using Dapper;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using RestSharp;
using spotifyloader.Models;
using spotifyloader.Models.API;
using spotifyloader.Models.Config;
using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;

namespace spotifyloader
{
    internal class Program
	{
		static Config config;
        class AuthResponse
		{
            public string access_token { get; set; }
            public string token_type { get; set; }
            public int expires_in { get; set; }
        }
		static DateTime apiKeyAge;
		static string currentApiKey = string.Empty;
		static string GetApiKey()
		{
			if(currentApiKey == string.Empty || (apiKeyAge + TimeSpan.FromMinutes(30)) < DateTime.Now)
			{
				string url = "https://accounts.spotify.com/api/token";
				Uri uri = new Uri(url);
				var options = new RestClientOptions($"{uri.Scheme}://{uri.Authority}");
				var client = new RestClient(options);
				client.AddDefaultParameter("grant_type", "client_credentials");
				client.AddDefaultParameter("client_id", config.ClientID);
				client.AddDefaultParameter("client_secret", config.ClientSecret);
				var request = new RestRequest(uri.AbsoluteUri);
				var response = client.Post(request);
				currentApiKey = JsonConvert.DeserializeObject<AuthResponse>(response.Content).access_token;
				apiKeyAge = DateTime.Now;
			}
            return currentApiKey;
        }
		static void LoadStreamingData()
		{
			int returnValue;

			Dictionary<int, Artist> artists = new Dictionary<int, Artist>();
            Dictionary<int, Album> albums = new Dictionary<int, Album>();
			Dictionary<int, Reason> reasons = new Dictionary<int, Reason>();
			Dictionary<int, Song> songs = new Dictionary<int, Song>();
			Dictionary<int, Platform> platforms = new Dictionary<int, Platform>();
			Dictionary<int, User> users = new Dictionary<int, User>();
			Stack<Data> data = new Stack<Data>();

			SqlConnection conn = new SqlConnection(config.ConnectionString);
			string sql = string.Empty;
			conn.Open();
			sql = "select * from dbo.Album";
			albums = conn.Query<Album>(sql).ToDictionary(k => k.ID, v => v);
			sql = "select * from dbo.Artist";
			artists = conn.Query<Artist>(sql).ToDictionary(k => k.ID, v => v);
			sql = "select * from dbo.Reason";
			reasons = conn.Query<Reason>(sql).ToDictionary(k => k.ID, v => v);
			sql = "select * from dbo.Song";
			songs = conn.Query<Song>(sql).ToDictionary(k => k.ID, v => v);
            sql = "select * from dbo.Platform";
            platforms = conn.Query<Platform>(sql).ToDictionary(k => k.ID, v => v);
			sql = "select * from dbo.[User]";
			users = conn.Query<User>(sql).ToDictionary(k => k.ID, v => v);
            conn.Close();
			conn.Open();

            var files = Directory.GetFiles(config.FolderLocation).Where(w => w.Contains(".json")).ToArray();
			foreach (var file in files)
			{
                IEnumerable<SongData> streamingData = JsonConvert.DeserializeObject<IEnumerable<SongData>>(File.ReadAllText(file));
				foreach (var dataObj in streamingData)
				{
					KeyValuePair<int, Artist> artist = artists.SingleOrDefault(a => a.Value.Name == dataObj.master_metadata_album_artist_name, default(KeyValuePair<int, Artist>));
					if (artist.Value is null)
					{
						sql = "insert into [dbo].[Artist]([Name]) output inserted.ID values (@Name)";
						returnValue = conn.QuerySingle<int>(sql, new { Name = dataObj.master_metadata_album_artist_name });
						Artist newArtist = new Artist { ID = returnValue, Name = dataObj.master_metadata_album_artist_name };
						artists[returnValue] = newArtist;
						artist = new KeyValuePair<int, Artist>(returnValue, newArtist);
                    }

					KeyValuePair<int, Album> album = albums.SingleOrDefault(a => a.Value.Name == dataObj.master_metadata_album_album_name, default(KeyValuePair<int, Album>));
                    if (album.Value is null)
                    {
                        sql = "insert into [dbo].[Album]([Name], [ArtistID]) output inserted.ID values (@Name, @ArtistID)";
                        returnValue = conn.QuerySingle<int>(sql, new 
						{
							Name = dataObj.master_metadata_album_album_name,
							ArtistID = artist.Key
						});
						Album newAlbum = new Album { ID = returnValue, Name = dataObj.master_metadata_album_album_name, ArtistID = artist.Key };
						albums[returnValue] = newAlbum;
						album = new KeyValuePair<int, Album>(returnValue, newAlbum);
                    }

					KeyValuePair<int, Reason> startReason = reasons.SingleOrDefault(sod => sod.Value.Name == dataObj.reason_start, default(KeyValuePair<int, Reason>));
                    sql = "insert into [dbo].[Reason]([Name]) output inserted.ID values (@Name)";
					if (startReason.Value is null)
					{
						returnValue = conn.QuerySingle<int>(sql, new { Name = dataObj.reason_start });
						Reason newStartReason = new Reason { ID = returnValue, Name = dataObj.reason_start };
						reasons[returnValue] = newStartReason;
						startReason = new KeyValuePair<int, Reason>(returnValue, newStartReason);
                    }

					KeyValuePair<int, Reason> endReason = reasons.SingleOrDefault(sod => sod.Value.Name == dataObj.reason_end, default(KeyValuePair<int, Reason>));
					if (endReason.Value is null)
					{
                        returnValue = conn.QuerySingle<int>(sql, new { Name = dataObj.reason_end });
						Reason newEndReason = new Reason { ID = returnValue, Name = dataObj.reason_end };
						reasons[returnValue] = newEndReason;
						endReason = new KeyValuePair<int, Reason>(returnValue, newEndReason);
                    }

					KeyValuePair<int, Song> song = songs.SingleOrDefault(sod => sod.Value.Track_Uri == dataObj.spotify_track_uri, default(KeyValuePair<int, Song>));
					if (song.Value is null) 
					{
						sql = "insert into [dbo].[Song]([Name], [ArtistID], [AlbumID], [Track_Uri]) output inserted.Id values (@Name, @ArtistID, @AlbumID, @Track_Uri)";
						returnValue = conn.QuerySingle<int>(sql, new 
						{ 
							Name = dataObj.master_metadata_track_name,
							ArtistID = artist.Key,
							AlbumID = album.Key,
							Track_Uri = dataObj.spotify_track_uri
						});
						Song newSong = new Song 
						{
							ID = returnValue,
							Name = dataObj.master_metadata_track_name,
							ArtistID = artist.Key,
							AlbumID = album.Key,
							Track_Uri = dataObj.spotify_track_uri 
						};
						songs[returnValue] = newSong;
						song = new KeyValuePair<int, Song>(returnValue, newSong);
					}

					KeyValuePair<int, Platform> platform = platforms.SingleOrDefault(sod => sod.Value.Name == dataObj.platform, default(KeyValuePair<int, Platform>));
					if (platform.Value is null)
					{
                        sql = "insert into [dbo].[Platform]([Name]) output inserted.ID values (@Name)";
						returnValue = conn.QuerySingle<int>(sql, new { Name = dataObj.platform });
						Platform newPlatform = new Platform { ID = returnValue, Name = dataObj.platform };
						platforms[returnValue] = newPlatform;
						platform = new KeyValuePair<int, Platform>(returnValue, newPlatform);
                    }

					KeyValuePair<int, User> user = users.SingleOrDefault(sod => sod.Value.Name == dataObj.username, default(KeyValuePair<int, User>));
					if (user.Value is null)
					{
						sql = "insert into [dbo].[User]([Name]) output inserted.ID values (@Name)";
                        returnValue = conn.QuerySingle<int>(sql, new { Name = dataObj.username });
                        User newUser = new User { ID = returnValue, Name = dataObj.username };
                        users[returnValue] = newUser;
                        user = new KeyValuePair<int, User>(returnValue, newUser);
                    }

					DateTime? date = null;
					if (dataObj.offline_timestamp is not null)
					{
						DateTime start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
						date = start.AddMilliseconds((double)dataObj.offline_timestamp).ToLocalTime();
					}


                    // https://www.c-sharpcorner.com/blogs/bulk-insert-in-sql-server-from-c-sharp
                    Data newData = new Data
					{
						EndTime = dataObj.ts,
						ms_played = dataObj.ms_played,
						ReasonStart = startReason.Key,
						ReasonEnd = endReason.Key,
						PlatformID = platform.Key,
						SongID = song.Key,
						Country = dataObj.conn_country,
						Skipped = dataObj.skipped,
						Offline = dataObj.offline,
						OfflineTimestamp = date,
						IncognitoMode = dataObj.incognito_mode,
						Shuffle = dataObj.shuffle,
						UserID = user.Key
					};

					sql = @"insert into [dbo].[Data]([EndTime]
								,[ms_played]
								,[ReasonStart]
								,[ReasonEnd]
								,[PlatformID]
								,[SongID]
								,[Country]
								,[Skipped]
								,[Offline]
								,[OfflineTimestamp]
								,[IncognitoMode]
								,[Shuffle]
								,[UserID])
							values (@EndTime, @ms_played, @ReasonStart, @ReasonEnd, @PlatformID, @SongID, @Country, @Skipped, @Offline, @OfflineTimestamp, @IncognitoMode, @Shuffle, @UserID)";

					conn.Execute(sql, new
					{
						newData.EndTime,
						newData.ms_played,
						newData.ReasonStart,
						newData.ReasonEnd,
						newData.PlatformID,
						newData.SongID,
						newData.Country,
						newData.Skipped,
						newData.Offline,
						newData.OfflineTimestamp,
						newData.IncognitoMode,
						newData.Shuffle,
						newData.UserID
					});
				}
            }
			conn.Dispose();
		}
		static void LoadAudioFeatureData()
		{
            using(SqlConnection conn = new SqlConnection(config.ConnectionString))
			{
				void LoadData(Dictionary<string, int> kvps)
				{
					string ids = "";
					foreach (var kvp in kvps)
					{
						ids += kvp.Key + ",";
					}
					ids = ids.Remove(ids.Length - 1);
					string url = "https://api.spotify.com/v1/audio-features?ids="+ids;
					Uri uri = new Uri(url);
					var options = new RestClientOptions($"{uri.Scheme}://{uri.Authority}");
					var client = new RestClient(options);
					client.AddDefaultHeader("Authorization", $"Bearer {GetApiKey()}");
					var request = new RestRequest(uri.AbsoluteUri);
					var response = client.Get(request);
					var features = JsonConvert.DeserializeObject<FeaturesResponse>(response.Content);
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
					foreach (var feature in features.audio_features)
					{
                        if(feature is null)
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
									FROM [SpotifyData].[dbo].[Song]
									where id not in (SELECT [SongID]
									FROM [SpotifyData].[dbo].[AudioFeature]) and Track_Uri is not null";
				songs = conn.Query<Song>(sql).ToDictionary(k => k.Track_Uri, v => v.ID);

				DateTime lastRequest = DateTime.Now;
				int loopCount = (int)Math.Ceiling(songs.Count / 100f);
				for (int i = 0; i < loopCount; i++)
				{
					if((lastRequest + TimeSpan.FromSeconds(2)) < DateTime.Now)
					{
						Thread.Sleep(TimeSpan.FromSeconds(2));
					}
                    LoadData(songs.Skip(i * 100).Take(100).ToDictionary(k => k.Key.Replace("spotify:track:", ""), v => v.Value));
                }
            }
		}
        static void Main(string[] args)
		{
			config = JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json"));
			//LoadStreamingData();
			//LoadAudioFeatureData();
		}
	}
}