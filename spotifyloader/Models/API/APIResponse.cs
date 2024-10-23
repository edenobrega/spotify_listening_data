namespace SpotifyLoader.Models.API
{
    internal class APIResponse
    {
        public List<FeatureResponse> audio_features { get; set; }
        public List<TrackResponse> tracks { get; set; }
    }
}
