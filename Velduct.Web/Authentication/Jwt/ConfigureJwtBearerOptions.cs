using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace Velduct.Web.Authentication.Jwt
{
    public class ConfigureJwtBearerOptions : IConfigureNamedOptions<JwtBearerOptions>
    {
        private readonly JwtSettings _jwtSettings;
        private readonly ILogger<ConfigureJwtBearerOptions> _logger;

        public ConfigureJwtBearerOptions(
            IOptions<JwtSettings> jwtOptions,
            ILogger<ConfigureJwtBearerOptions> logger)
        {
            _jwtSettings = jwtOptions.Value;
            _logger = logger;
        }

        public void Configure(string name, JwtBearerOptions options)
        {
            options.TimeProvider = TimeProvider.System;

            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuerSigningKey = true,
                IssuerSigningKeys = _jwtSettings.ServerKeys
                    .Select(k => (SecurityKey)new SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(k)))
                    .ToList(),

                ValidateIssuer = true,
                // Issuer is client-defined; we only verify it's not empty
                IssuerValidator = (issuer, _, _) =>
                {
                    if (string.IsNullOrWhiteSpace(issuer))
                        throw new SecurityTokenInvalidIssuerException(
                            "Issuer (iss) claim is required but was not provided.");
                    return issuer;
                },

                ValidateAudience = true,
                ValidAudience = _jwtSettings.Audience,

                ValidateLifetime = true,
                ClockSkew = TimeSpan.FromMinutes(_jwtSettings.ClockSkewMinutes)
            };

            // WebSocket clients cannot send Authorization headers; tokens arrive via ?access_token query parameter
            options.Events = new JwtBearerEvents
            {
                OnMessageReceived = ctx =>
                {
                    var token = ctx.Request.Query["access_token"].FirstOrDefault();
                    if (!string.IsNullOrEmpty(token))
                        ctx.Token = token;
                    return Task.CompletedTask;
                },

                // Log failures at Debug level; WsTokenHandler logs actual rejections with context
                OnAuthenticationFailed = ctx =>
                {
                    _logger.LogDebug("JWT validation failed: {Error}", ctx.Exception.Message);
                    return Task.CompletedTask;
                }
            };
        }

        public void Configure(JwtBearerOptions options) => Configure(Options.DefaultName, options);
    }
}
