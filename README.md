This project is mainly to build a bridge to OpenShock's SignalR API to consume events as Triggers for Streamerbot

I.E., if you get shocked, you can now have streamerbot react to it

## Build
`dotnet build -c Release`
Then copy the dll from the Release merged directory

## StreamerBot Usage
- Close StreamerBot
- Place `OpenShockBridge.dll` into `<StreamerBotInstall>/dlls/` install folder
- Restart back up StreamerBot
- Import `openshock.sb`
- In `OpenShock [0] - Setup`:
  - Open `Execute Code` subaction
  - Click `Find Refs`
- In `OpenShock [2] - Listener`:
  - Open `Execute Code` subaction
  - Click `Find Refs`
- Set Global Variable `openshock_token` to your API Token (see comments in Setup SubActions)
  - https://openshock.app/settings/api-tokens
- Restart StreamerBot
- See `OpenShock [3] Example Trigger` for example
  - You should now have Triggers for:
    - Shock, Vibrate, Sound, Stop actions
      - These will have:
        - %shockerName% name of triggering shocker
        - %typeName% event name, should always match the trigger type unless you do any console log
        - %duration% time in milliseconds
        - %intensity% 0-100 representing the intensity percentage
    - Connected, which should happen on boot or after manual reconnect
      - to manually try reconnecting if needed, go to Setup action and run the Test trigger
  - Search for them by typing "OpenShock" in the triggers search
  