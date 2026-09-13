using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OpenShockBridge
{
    internal static class UserAgentWebSocket
    {
        // websocket guid
        private const string GuidRfc6455 = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        private const int BufferSize = 8192;

        public static async Task<WebSocket> ConnectAsync(
            Uri uri,
            string userAgent,
            IEnumerable<KeyValuePair<string, string>> headers,
            TimeSpan keepAliveInterval,
            CancellationToken cancellationToken)
        {
            bool secure = uri.Scheme is "wss" or "https";
            int port = uri.IsDefaultPort ? (secure ? 443 : 80) : uri.Port;

            var tcp = new TcpClient { NoDelay = true };
            Stream stream = null;

            using (cancellationToken.Register(() => { try { tcp.Close(); }
                       catch {
                           // ignored
                       }
                   }))
            {
                try
                {
                    await tcp.ConnectAsync(uri.Host, port).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    stream = tcp.GetStream();
                    if (secure)
                    {
                        var ssl = new SslStream(stream, false);
                        await ssl.AuthenticateAsClientAsync(
                            uri.Host, null, SslProtocols.Tls12, false).ConfigureAwait(false);
                        stream = ssl;
                    }

                    string key = CreateKey();
                    byte[] request = Encoding.ASCII.GetBytes(BuildRequest(uri, port, key, userAgent, headers));
                    await stream.WriteAsync(request, 0, request.Length, cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

                    string response = await ReadHandshakeResponseAsync(stream, cancellationToken).ConfigureAwait(false);
                    ValidateResponse(response, key);

                    return WebSocket.CreateClientWebSocket(
                        stream,
                        null,
                        BufferSize,
                        BufferSize,
                        keepAliveInterval,
                        false,
                        WebSocket.CreateClientBuffer(BufferSize, BufferSize));
                }
                catch
                {
                    try {
                        stream?.Dispose();
                    }
                    catch {
                        // ignored
                    }

                    try { tcp.Close(); }
                    catch {
                        // ignored
                    }

                    throw;
                }
            }
        }

        private static string BuildRequest(
            Uri uri, int port, string key, string userAgent,
            IEnumerable<KeyValuePair<string, string>> headers)
        {
            var sb = new StringBuilder();
            sb.Append("GET ").Append(uri.PathAndQuery).Append(" HTTP/1.1\r\n");
            sb.Append("Host: ").Append(uri.Host).Append(uri.IsDefaultPort ? "" : ":" + port).Append("\r\n");
            sb.Append("Upgrade: websocket\r\n");
            sb.Append("Connection: Upgrade\r\n");
            sb.Append("Sec-WebSocket-Key: ").Append(key).Append("\r\n");
            sb.Append("Sec-WebSocket-Version: 13\r\n");
            if (!string.IsNullOrEmpty(userAgent))
                sb.Append("User-Agent: ").Append(userAgent).Append("\r\n");

            if (headers != null)
            {
                foreach (var kv in headers)
                {
                    if (string.IsNullOrEmpty(kv.Key) || kv.Value == null) continue;
                    if (kv.Key.IndexOfAny(new[] { '\r', '\n' }) >= 0 ||
                        kv.Value.IndexOfAny(new[] { '\r', '\n' }) >= 0)
                        throw new ArgumentException("Header '" + kv.Key + "' contains a line break.");
                    sb.Append(kv.Key).Append(": ").Append(kv.Value).Append("\r\n");
                }
            }

            sb.Append("\r\n");
            return sb.ToString();
        }

        private static string CreateKey()
        {
            var bytes = new byte[16];
            using (var rng = new RNGCryptoServiceProvider())
                rng.GetBytes(bytes);
            return Convert.ToBase64String(bytes);
        }

        private static string ExpectedAccept(string key)
        {
            using (var sha1 = SHA1.Create())
                return Convert.ToBase64String(sha1.ComputeHash(Encoding.ASCII.GetBytes(key + GuidRfc6455)));
        }

        private static async Task<string> ReadHandshakeResponseAsync(Stream stream, CancellationToken cancellationToken)
        {
            var sb = new StringBuilder();
            var one = new byte[1];
            int matched = 0;

            while (matched < 4)
            {
                int read = await stream.ReadAsync(one, 0, 1, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    throw new WebSocketException("Connection closed during the WebSocket handshake.");

                char c = (char)one[0];
                sb.Append(c);

                if (matched is 0 or 2 && c == '\r') matched++;
                else if (matched is 1 or 3 && c == '\n') matched++;
                else matched = c == '\r' ? 1 : 0;

                if (sb.Length > 16 * 1024)
                    throw new WebSocketException("WebSocket handshake response headers too large.");
            }

            return sb.ToString();
        }

        private static void ValidateResponse(string response, string key)
        {
            string[] lines = response.Split(["\r\n"], StringSplitOptions.RemoveEmptyEntries);
            string statusLine = lines.Length > 0 ? lines[0] : "";

            if (statusLine.IndexOf(" 101", StringComparison.Ordinal) < 0)
                throw new WebSocketException("WebSocket handshake failed: " + statusLine.Trim());

            string accept = null;
            for (int i = 1; i < lines.Length; i++)
            {
                int colon = lines[i].IndexOf(':');
                if (colon <= 0) continue;
                if (lines[i].Substring(0, colon).Trim()
                        .Equals("Sec-WebSocket-Accept", StringComparison.OrdinalIgnoreCase))
                {
                    accept = lines[i].Substring(colon + 1).Trim();
                    break;
                }
            }

            if (!string.Equals(accept, ExpectedAccept(key), StringComparison.Ordinal))
                throw new WebSocketException("WebSocket handshake failed: invalid Sec-WebSocket-Accept.");
        }
    }
}
