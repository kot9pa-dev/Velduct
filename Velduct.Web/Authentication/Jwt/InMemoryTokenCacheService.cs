using Microsoft.Extensions.Caching.Memory;

namespace Velduct.Web.Authentication.Jwt
{
    public class InMemoryTokenCacheService : ITokenCacheService
    {
        private readonly IMemoryCache _memoryCache;
        private readonly ILogger<InMemoryTokenCacheService> _logger;
        private const string Prefix = "jti_state_";

        public InMemoryTokenCacheService(
            IMemoryCache memoryCache,
            ILogger<InMemoryTokenCacheService> logger)
        {
            _memoryCache = memoryCache;
            _logger = logger;
        }

        public JtiState GetState(string jti)
        {
            if (_memoryCache.TryGetValue(Prefix + jti, out JtiState state))
                return state;

            return JtiState.None;
        }

        public void SetState(string jti, JtiState state, TimeSpan ttl)
        {
            var options = new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = ttl
            };
            _memoryCache.Set(Prefix + jti, state, options);
            _logger.LogDebug("JTI {Jti} -> {State} (ttl={Ttl})", jti, state, ttl);
        }

        public void Remove(string jti)
        {
            _memoryCache.Remove(Prefix + jti);
            _logger.LogDebug("Removed JTI {Jti} from cache", jti);
        }
    }
}
