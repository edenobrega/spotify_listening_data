using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpotifyLoader.Models.Config
{
	internal class Config
	{
        [JsonProperty("connectionString")]
        public string ConnectionString { get; set; }
        [JsonProperty("client_id")]
        public string ClientID { get; set; }
        [JsonProperty("client_secret")]
		public string ClientSecret { get; set; }
		[JsonProperty("file_directory")] 
		public string Directory { get; set; }
        [JsonProperty("song_album_data_directory")]
        public string SongAlbumDirectory { get; set; }
        [JsonProperty("song_album_data_file_name")]
        public string SongAlbumFileName { get; set; }
        [JsonProperty("bulk_insert_size")] 
		public int BulkSize { get; set; }
	}
}
