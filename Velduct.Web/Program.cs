using Velduct.Web.Authentication;
using Velduct.Web.Authentication.Jwt;
using Velduct.Web.Configuration;
using Velduct.Web.Core;
using Velduct.Web.Services;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using NLog;
using NLog.Web;

var logger = LogManager.Setup().LoadConfigurationFromAppSettings().GetCurrentClassLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Logging.ClearProviders();
    builder.Host.UseNLog();

    builder.Configuration.AddEnvironmentVariables();

    // Set Kestrel buffer to chunk size to avoid multiple flushes per chunk send
    var cfgChunkSize = int.Parse(
        builder.Configuration[AppConstants.ConfigChunkSizeBytes] ?? "4194304");
    var cfgKestrelOverhead = int.Parse(
        builder.Configuration[AppConstants.ConfigKestrelBufferOverhead] ?? "4096");

    builder.WebHost.ConfigureKestrel(serverOptions =>
    {
        serverOptions.Limits.MaxResponseBufferSize = cfgChunkSize + cfgKestrelOverhead;
    });

    builder.Services.Configure<TransferOptions>(
        builder.Configuration.GetSection(TransferOptions.SectionName));

    builder.Services.Configure<JwtSettings>(
        builder.Configuration.GetSection(JwtSettings.SectionName));

    // Data and Temp must be on same volume for File.Move
    var dataDir = builder.Configuration[AppConstants.ConfigDataDirectory]!;
    var tempDir = builder.Configuration[AppConstants.ConfigTempDirectory]!;

    Directory.CreateDirectory(dataDir);
    Directory.CreateDirectory(tempDir);

    string dataRoot = Path.GetPathRoot(Path.GetFullPath(dataDir)) ?? "";
    string tempRoot = Path.GetPathRoot(Path.GetFullPath(tempDir)) ?? "";

    if (!string.Equals(dataRoot, tempRoot, StringComparison.OrdinalIgnoreCase))
    {
        logger.Fatal(
            "FATAL: DataDirectory ({DataRoot}) and TempDirectory ({TempRoot}) are on different volumes! File.Move will fail.",
            dataRoot, tempRoot);
        throw new InvalidOperationException(
            $"DataDirectory ({dataRoot}) and TempDirectory ({tempRoot}) are on different volumes. Fix appsettings.json.");
    }

    builder.Services.AddMemoryCache();
    builder.Services.AddSingleton<ITokenCacheService, InMemoryTokenCacheService>();

    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer();

    builder.Services.ConfigureOptions<ConfigureJwtBearerOptions>();

    builder.Services.AddSingleton<IAuthorizationHandler, WsTokenHandler>();
    builder.Services.AddAuthorization(options =>
    {
        options.AddPolicy(AuthPolicies.WsConnect, policy =>
        {
            policy.AuthenticationSchemes.Add(JwtBearerDefaults.AuthenticationScheme);
            policy.Requirements.Add(new WsTokenRequirement());
        });
    });

    builder.Services.AddSingleton<FileCacheStore>();
    builder.Services.AddSingleton<MemoryManagerService>();
    builder.Services.AddSingleton<Velduct.Web.Infrastructure.ConnectionManager>();
    builder.Services.AddSingleton<BroadcastService>();
    builder.Services.AddSingleton<GlobalDeleteQueueService>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<GlobalDeleteQueueService>());
    builder.Services.AddSingleton<GlobalBroadcastFlusherService>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<GlobalBroadcastFlusherService>());
    builder.Services.AddSingleton<Velduct.Web.Infrastructure.ConnectionTracker>();
    builder.Services.AddHostedService<ConnectionHealthMonitorService>();

    builder.Services.AddScoped<StorageManager>();
    builder.Services.AddScoped<SyncOrchestrator>();
    builder.Services.AddScoped<ArchiveProcessor>();
    builder.Services.AddScoped<DiskWorkerService>();
    builder.Services.AddScoped<ServerArchiveSender>();
    builder.Services.AddScoped<WebSocketFrameReader>();
    builder.Services.AddScoped<WebSocketHandlerService>();

    var app = builder.Build();

    using (var startupScope = app.Services.CreateScope())
    {
        var manager = startupScope.ServiceProvider.GetRequiredService<StorageManager>();
        await manager.InitializeFullScanAsync();
    }

    var cfgKeepAlive = int.Parse(
        builder.Configuration[AppConstants.ConfigKeepAliveSeconds] ?? "15");

    app.UseWebSockets(new Microsoft.AspNetCore.Builder.WebSocketOptions
    {
        KeepAliveInterval = TimeSpan.FromSeconds(cfgKeepAlive)
    });

    app.UseAuthentication();
    app.UseAuthorization();

    app.Map("/ws", async context =>
    {
        var manager = context.RequestServices.GetRequiredService<StorageManager>();
        if (!manager.IsReady)
        {
            context.Response.StatusCode = 503;
            return;
        }

        var authService = context.RequestServices.GetRequiredService<IAuthorizationService>();
        var authResult = await authService.AuthorizeAsync(context.User, context, AuthPolicies.WsConnect);

        if (!authResult.Succeeded)
        {
            context.Response.StatusCode = 401;
            return;
        }

        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = 400;
            return;
        }

        using var ws = await context.WebSockets.AcceptWebSocketAsync();
        using var scope = context.RequestServices.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<WebSocketHandlerService>();
        await handler.HandleAsync(ws, context.RequestAborted);
    });

    logger.Info("Velduct Web started");
    app.Run();
}
catch (Exception ex)
{
    logger.Error(ex, "Application stopped due to exception.");
    throw;
}
finally
{
    LogManager.Shutdown();
}
