namespace Velduct.Web.Authentication.Jwt
{
    public interface ITokenCacheService
    {
        JtiState GetState(string jti);
        void SetState(string jti, JtiState state, TimeSpan ttl);
        void Remove(string jti);
    }
}
