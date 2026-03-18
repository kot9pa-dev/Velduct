using Velduct.Web.Authentication.Jwt;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;
using System.IdentityModel.Tokens.Jwt;

namespace Velduct.Web.Authentication
{
    /// <summary>
    /// Enforces single-use JWT tokens for WebSocket connections.
    /// Prevents replay attacks: a stolen token cannot establish two connections.
    /// </summary>
    public class WsTokenHandler : AuthorizationHandler<WsTokenRequirement>
    {
        private const string WsPath = "/ws";

        private readonly ITokenCacheService _tokenCache;
        private readonly JwtSettings _jwtSettings;
        private readonly ILogger<WsTokenHandler> _logger;

        public WsTokenHandler(
            ITokenCacheService tokenCache,
            IOptions<JwtSettings> jwtOptions,
            ILogger<WsTokenHandler> logger)
        {
            _tokenCache = tokenCache;
            _jwtSettings = jwtOptions.Value;
            _logger = logger;
        }

        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context,
            WsTokenRequirement requirement)
        {
            if (context.Resource is not HttpContext httpContext)
            {
                _logger.LogWarning("WsTokenHandler: no HttpContext in resource — impossible path");
                context.Fail();
                return Task.CompletedTask;
            }

            var path = httpContext.Request.Path.Value ?? "";

            // This handler protects only /ws endpoint
            if (!path.Equals(WsPath, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("WsTokenHandler: unexpected path {Path} — only {WsPath} is allowed", path, WsPath);
                context.Fail();
                return Task.CompletedTask;
            }

            // JWT authentication must have passed
            if (context.User.Identity is not { IsAuthenticated: true })
            {
                _logger.LogDebug("WsTokenHandler: unauthenticated request to {Path}", path);
                context.Fail();
                return Task.CompletedTask;
            }

            // JTI claim is required to enforce single-use policy
            var jti = context.User.FindFirst(JwtRegisteredClaimNames.Jti)?.Value;
            if (string.IsNullOrEmpty(jti))
            {
                _logger.LogWarning("WsTokenHandler: token missing required jti claim");
                context.Fail();
                return Task.CompletedTask;
            }

            var issuer = context.User.FindFirst(JwtRegisteredClaimNames.Iss)?.Value ?? "unknown";
            var currentState = _tokenCache.GetState(jti);
            var ttl = TimeSpan.FromMinutes(_jwtSettings.UsedTokenStoreMinutes);

            if (currentState == JtiState.None)
            {
                // First use: allow and mark as used
                _tokenCache.SetState(jti, JtiState.Connected, ttl);
                _logger.LogInformation("WsTokenHandler: accepted — issuer={Issuer} jti={Jti}", issuer, jti);
                context.Succeed(requirement);
            }
            else
            {
                // Token reuse detected
                _logger.LogWarning(
                    "WsTokenHandler: REPLAY DENIED — issuer={Issuer} jti={Jti} state={State}",
                    issuer, jti, currentState);
                context.Fail();
            }

            return Task.CompletedTask;
        }
    }
}
