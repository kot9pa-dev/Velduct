using System.Net.WebSockets;

namespace Velduct.Web.Interfaces;

public interface IWebSocketHandlerService
{
    Task HandleAsync(WebSocket webSocket, CancellationToken ct);
}
