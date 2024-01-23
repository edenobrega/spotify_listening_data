using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace spotifyloader.Models.Config
{
	internal class Config
	{
        [JsonProperty("connectionString")]
        public string ConnectionString { get; set; }
        [JsonProperty("client_id")]
        public string ClientID { get; set; }
        [JsonProperty("client_secret")]
		public string ClientSecret { get; set; }
	}
}
