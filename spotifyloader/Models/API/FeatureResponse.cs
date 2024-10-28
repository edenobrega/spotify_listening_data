using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpotifyLoader.Models.API
{
    internal class FeatureResponse
    {
        public List<FeatureItem> audio_features { get; set; }
    }
    internal class FeatureItem
    {
        [ColumnName("SongID")]
        public string ID { get; set; }
        public float Acousticness { get; set; }
        public float Danceability { get; set; }
        public float Energy { get; set; }
        public float Instrumentalness { get; set; }
        public int Key { get; set; }
        public float Liveness { get; set; }
        public float Loudness { get; set; }
        public int Mode { get; set; }
        public float Speechiness { get; set; }
        public float Tempo { get; set; }
        public int TimeSignature { get; set; }
        public float Valence { get; set; }
    }
}
