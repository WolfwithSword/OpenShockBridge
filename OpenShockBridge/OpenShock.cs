using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace OpenShockBridge
{
    public static class OpenShock
    {
        private const string DefaultUrl = "https://api.openshock.app/1/hubs/user";

        private static readonly object Gate = new object();
        private static HubConnection _hub;

        private static Action<string, string[]> _onMessage;
        private static Action<string> _onLog;

        private static readonly Dictionary<string, int> Methods = new Dictionary<string, int>
        {
            { "Welcome", 1 },
            { "DeviceStatus", 1 },
            { "Log", 2 },
            { "DeviceUpdate", 2 },
            { "OtaInstallStarted", 3 },
            { "OtaInstallProgress", 4 },
            { "OtaInstallFailed", 4 },
            { "OtaRollback", 2 },
            { "OtaInstallSucceeded", 2 },
        };

        public static bool IsConnected
        {
            get
            {
                var h = _hub;
                return h != null && h.State == HubConnectionState.Connected;
            }
        }

        public static string State
        {
            get
            {
                var h = _hub;
                return h == null ? "None" : h.State.ToString();
            }
        }

        public static bool Connect(
            string token,
            Action<string, string[]> onMessage,
            Action<string> onLog = null,
            string url = null,
            string userAgent = "Streamerbot-OpenShock/1.0")
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                onLog?.Invoke("[OpenShock] No API token provided.");
                return false;
            }

            lock (Gate)
            {
                if (IsConnected)
                {
                    onLog?.Invoke("[OpenShock] Already connected.");
                    return true;
                }

                if (_hub != null)
                {
                    try { _hub.DisposeAsync().GetAwaiter().GetResult(); }
                    catch {
                        // ignored
                    }

                    _hub = null;
                }

                _onMessage = onMessage;
                _onLog = onLog;

                IHubConnectionBuilder builder = new HubConnectionBuilder()
                    .WithUrl(string.IsNullOrWhiteSpace(url) ? DefaultUrl : url,  HttpTransportType.WebSockets, options => {
                        options.Transports = HttpTransportType.WebSockets;
                        options.SkipNegotiation = true;
                        options.Headers.Add("Open-Shock-Token", token);
                        options.Headers.Add("X-SignalR-User-Agent", userAgent);

                        options.WebSocketFactory = async (context, cancellationToken) =>
                        {
                            var target = (string.IsNullOrWhiteSpace(url) ? DefaultUrl : url)
                                .Replace("https://", "wss://").Replace("http://", "ws://");
                            Log("[OpenShock] WS connecting to: " + target);

                            try
                            {
                                System.Net.ServicePointManager.SecurityProtocol =
                                    System.Net.SecurityProtocolType.Tls12 | System.Net.SecurityProtocolType.Tls11 | System.Net.SecurityProtocolType.Tls;

                                var wsHeaders = new Dictionary<string, string>
                                {
                                    { "Open-Shock-Token", token },
                                    { "X-SignalR-User-Agent", userAgent }
                                };

                                var ws = await UserAgentWebSocket.ConnectAsync(
                                    new Uri(target),
                                    userAgent,
                                    wsHeaders,
                                    TimeSpan.FromSeconds(15),
                                    cancellationToken);

                                Log("[OpenShock] WS connected.");
                                return ws;
                            }
                            catch (Exception ex)
                            {
                                Log("[OpenShock] WS connect error: " + ex.GetType().Name + ": " + ex.Message);
                                var inner = ex.InnerException;
                                while (inner != null)
                                {
                                    Log("[OpenShock]   inner: " + inner.GetType().Name + ": " + inner.Message);
                                    inner = inner.InnerException;
                                }
                                throw;
                            }
                        };
                    });
                
                builder.Services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
                builder.Services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

                HubConnection connection = builder.Build();

                RegisterHandlers(connection);

                connection.Reconnecting += err => { Log("[OpenShock] Reconnecting..."); return Task.CompletedTask; };
                connection.Reconnected  += id  => { Log("[OpenShock] Reconnected: " + id); return Task.CompletedTask; };
                connection.Closed       += err => { Log("[OpenShock] Connection closed." + (err != null ? " " + err.Message : "")); return Task.CompletedTask; };

                try
                {
                    connection.StartAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    onLog?.Invoke("[OpenShock] Connect failed: " + ex.Message);
                    try { connection.DisposeAsync().GetAwaiter().GetResult(); }
                    catch {
                        // ignored
                    }

                    return false;
                }

                _hub = connection;
                onLog?.Invoke("[OpenShock] Connected. State=" + connection.State);
                return true;
            }
        }

        public static bool Control(
            string shockerId,
            int type,
            int intensity,
            int duration,
            bool exclusive = false,
            string customName = "Streamer.bot")
        {
            var shocks = new[]
            {
                new
                {
                    id = shockerId,
                    type = type,
                    intensity = intensity,
                    duration = duration,
                    exclusive = exclusive
                }
            };
            return InvokeControl(shocks, customName);
        }

        public static bool ControlJson(string commandsJsonArray, string customName = "Streamer.bot")
        {
            object shocks;
            try
            {
                shocks = JsonSerializer.Deserialize<JsonElement>(commandsJsonArray);
            }
            catch (Exception ex)
            {
                Log("[OpenShock] ControlJson parse failed: " + ex.Message);
                return false;
            }
            return InvokeControl(shocks, customName);
        }

        private static bool InvokeControl(object shocks, string customName)
        {
            var h = _hub;
            if (h is not { State: HubConnectionState.Connected })
            {
                Log("[OpenShock] Not connected; Control ignored.");
                return false;
            }

            try
            {
                h.InvokeAsync("ControlV2", shocks, customName).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception ex)
            {
                Log("[OpenShock] Control failed: " + ex.Message);
                return false;
            }
        }

        public static bool DeviceCommand(string method, string deviceId)
        {
            var h = _hub;
            if (h is not { State: HubConnectionState.Connected })
            {
                Log("[OpenShock] Not connected; " + method + " ignored.");
                return false;
            }

            try
            {
                h.InvokeAsync(method, Guid.Parse(deviceId)).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception ex)
            {
                Log("[OpenShock] " + method + " failed: " + ex.Message);
                return false;
            }
        }

        public static void Disconnect()
        {
            lock (Gate)
            {
                HubConnection h = _hub;
                _hub = null;
                if (h == null) return;

                try { h.StopAsync().GetAwaiter().GetResult(); }
                catch {
                    // ignored
                }

                try { h.DisposeAsync().GetAwaiter().GetResult(); }
                catch {
                    // ignored
                }

                Log("[OpenShock] Disconnected.");
            }
        }

        private static void RegisterHandlers(HubConnection c)
        {
            foreach (KeyValuePair<string, int> kv in Methods)
            {
                string method = kv.Key;
                int argCount = kv.Value;

                var types = new Type[argCount];
                for (int i = 0; i < argCount; i++) types[i] = typeof(object);

                c.On(method, types, args =>
                {
                    try
                    {
                        var jsonArgs = new string[args.Length];
                        for (int i = 0; i < args.Length; i++)
                            jsonArgs[i] = args[i] == null ? null : JsonSerializer.Serialize(args[i]);

                        _onMessage?.Invoke(method, jsonArgs);
                    }
                    catch (Exception ex)
                    {
                        Log("[OpenShock] Handler '" + method + "' error: " + ex.Message);
                    }
                    return Task.CompletedTask;
                });
            }
        }

        private static void Log(string msg)
        {
            try { _onLog?.Invoke(msg); }
            catch {
                // ignored
            }
        }

        public static List<Dictionary<string, object>> ParseControlLogs(string senderJson, string logsJson)
        {
            var rows = new List<Dictionary<string, object>>();

            string senderName = "Someone";
            try
            {
                using (var sd = JsonDocument.Parse(senderJson))
                    senderName = StrProp(sd.RootElement, "customName")
                              ?? StrProp(sd.RootElement, "name")
                              ?? "Someone";
            }
            catch {
                // ignored
            }

            try {
                using var ld = JsonDocument.Parse(logsJson);
                foreach (JsonElement log in ld.RootElement.EnumerateArray())
                {
                    int type = ReadTypeVal(log);

                    string shockerId = "", shockerName = "shocker";
                    JsonElement sh;
                    if (TryGetProp(log, "shocker", out sh))
                    {
                        shockerId   = StrProp(sh, "id") ?? "";
                        shockerName = StrProp(sh, "name") ?? "shocker";
                    }

                    rows.Add(new Dictionary<string, object>
                    {
                        ["senderName"] = senderName,
                        ["shockerId"] = shockerId,
                        ["shockerName"] = shockerName,
                        ["type"] = type,
                        ["typeName"] = TypeNameVal(type),
                        ["intensity"] = IntProp(log, "intensity"),
                        ["duration"] = IntProp(log, "duration")
                    });
                }
            }
            catch (Exception ex)
            {
                Log("[OpenShock] ParseControlLogs failed: " + ex.Message + " | raw: " + logsJson);
            }

            return rows;
        }

        public static string TypeName(int type) { return TypeNameVal(type); }

        private static int ReadTypeVal(JsonElement log)
        {
            JsonElement v;
            if (!TryGetProp(log, "type", out v)) return -1;

            if (v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out int i))
                return i;

            if (v.ValueKind == JsonValueKind.String)
            {
                string s = (v.GetString() ?? "").Trim();
                if (s.Equals("Stop", StringComparison.OrdinalIgnoreCase)) return 0;
                if (s.Equals("Shock", StringComparison.OrdinalIgnoreCase)) return 1;
                if (s.Equals("Vibrate", StringComparison.OrdinalIgnoreCase)) return 2;
                if (s.Equals("Sound", StringComparison.OrdinalIgnoreCase)) return 3;
            }
            return -1;
        }

        private static string TypeNameVal(int t)
        {
            switch (t)
            {
                case 0: return "Stop";
                case 1: return "Shock";
                case 2: return "Vibrate";
                case 3: return "Sound";
                default: return "Type" + t;
            }
        }

        private static bool TryGetProp(JsonElement e, string prop, out JsonElement val)
        {
            val = default(JsonElement);
            if (e.ValueKind != JsonValueKind.Object) return false;
            foreach (var p in e.EnumerateObject())
                if (string.Equals(p.Name, prop, StringComparison.OrdinalIgnoreCase))
                { val = p.Value; return true; }
            return false;
        }

        private static string StrProp(JsonElement e, string prop)
        {
            JsonElement v;
            return TryGetProp(e, prop, out v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        }

        private static int IntProp(JsonElement e, string prop)
        {
            JsonElement v; int i;
            return TryGetProp(e, prop, out v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out i) ? i : 0;
        }
    }
}