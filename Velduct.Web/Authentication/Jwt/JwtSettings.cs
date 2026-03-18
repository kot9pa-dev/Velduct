namespace Velduct.Web.Authentication.Jwt
{
    public class JwtSettings
    {
        public const string SectionName = "JwtSettings";

        public int ClockSkewMinutes { get; set; } = 1;

        public int UsedTokenStoreMinutes { get; set; } = 6;

        public string Audience { get; set; } = "VelductServer";

        public List<string> ServerKeys { get; set; } = new();
    }
}
