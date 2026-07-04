# Redionet

Synchronized, server-wide music streaming with ComputerCraft.

This fork ([sevennnoff/redionet](https://github.com/sevennnoff/redionet)) extends [Rypo/redionet](https://github.com/Rypo/redionet) with a **centralized control model**: one server owns playback, one optional controller runs the UI, and passive speaker clients only play synchronized audio.

## Why this fork?

| | Upstream (Rypo) | This fork |
|---|---|---|
| **Roles** | Every computer can search, queue, and control playback | **Server** streams · **Controller** controls · **Client** listens |
| **Control** | Open — any connected client can change the queue | **Password-protected** — only the authorized controller |
| **Volume / loop / skip** | Per-client or first-come | **Server-wide**, set from the controller |
| **Client UI** | Full player on every machine | Passive clients show status only; controller has the full UI |
| **Updates** | Manual per machine | `rn update` on the server pushes to all devices |
| **Protocol** | `PROTO_*` | `RDN:*:v5` (legacy clients can be flushed with `rn killlegacy`) |

Audio sync uses the same chunk-barrier algorithm as upstream Rypo (server decodes, rednet broadcast, speaker-buffer pacing). Fork-specific work focuses on roles, auth, and remote management — not experimental sync modes.

**Features**
- Shared audio streaming anywhere in a server, even across dimensions!
- Scrollable Search Results. Use arrow keys to scroll results
- Keyboard Controls. Search and add songs mouse-free
- Song Announcements. Notifications sent out for each new track
- Chat Commands. Stay up to date and in sync by simply typing in the chat or terminal
- Pocket Features. Search and add songs from anywhere, at any time

![UI_demo](assets/ui_demo.gif)


## Installation
Option 1: `pastebin`
```sh
pastebin run Fe4MDC1Y
```

Option 2: `wget` 
```sh
wget run https://raw.githubusercontent.com/sevennnoff/redionet/refs/heads/main/install.lua
```

## Setup

You'll need exactly 1 Server computer, as many passive Client computers as you'd like, and one optional Controller computer for remote control.

Computers are designated as Server, Client, or Controller upon running the Installation script.

### Minimum
**Server** - Advanced Computer/Turtle with Ender modem

**Client(s)** - Advanced Computer/Turtle with Ender modem + Speaker

**Controller** - Advanced Computer/Pocket Computer with Ender modem. Speaker not required.

### Preferred
_[Advanced Peripherals](https://docs.advanced-peripherals.de/latest/) (AP) mod required for some peripherals._

**Both Server and Clients** 
- A nearby chunk loader (e.g., [chunky turtle](https://docs.advanced-peripherals.de/latest/turtles/chunky_turtle/))

The **server chunk must be loaded** at all times for clients to function properly.
Client chunks do not all _need_ to be force loaded, however, frequently cycling chunks in/out will likely result in audio stuttering/synchronization issues. 

**Server**
- Advanced Monitors - better presentation of debug/client health info (at least 5w x 3h)
- [Chat Box](https://docs.advanced-peripherals.de/latest/peripherals/chat_box/) - enables chat commands + song announcements (chat message)
- [Player Detector](https://docs.advanced-peripherals.de/latest/peripherals/player_detector/) - enables fancy song announcements (toast notifications)

**Clients**: no additional peripheral recommendations

**Controller**: no additional peripheral recommendations


### Additional (Optional)

**Pocket Controller**: Advanced Pocket Computer with Ender modem
- Allows you to control playback and add songs on the go.
  
At the moment, passive audio Clients still need a speaker. Pocket computers are best used as Controllers unless your CC:T version supports enough attached peripherals for modem + speaker audio.

### Settings

`redionet.log_level` - The lowest severity message to display on the Server screen. \
1=DEBUG, 2=INFO, 3=WARN, 4=ERROR (default=3).

Non-user Settings (auto-assigned during installation) - `redionet.device_type` and `redionet.run_on_boot`. Do not manually change these - use the install script instead.

### Control Password

During Server installation, Redionet asks for a control password and stores it encrypted in `.redionet.auth`. Server playback, queue, loop mode, and volume controls only work from the Controller that entered this password. Re-run the installer as Server or delete `.redionet.auth` and restart the server to set a new one.

Volume is server-wide. Changing it on the authorized Controller updates all connected Clients.

## Chat Commands
- `rn help`   - Prints each chat command with a brief description in the server terminal.  
- `rn reboot` - Reboots server and all client computers. Server/client programs will not auto start unless run on startup is selected during installation.
- `rn reload` - Attempts to reload server and all clients without shutting down. Less reliable than _rn reboot_ but doesn't require run on start. 
- `rn update` - Pulls the latest code from GitHub and reloads if changes are detected.
- `rn sync`   - Forces clients to resynchronize audio streams.
- `rn killlegacy` - Forces clients still on the old protocol set to reboot.

### Command Usage

**With AP mod** and a Chat Box peripheral attached,
- Song announcements still use chat. Chat commands are not accepted from regular player chat because control is limited to the authorized Controller.

**Without AP mod**, there are two alternative ways to use commands.
1. If you're next to the server computer, type the command into the server's built-in `CMD>` line.
   
2. From the currently authorized Controller computer, open the `lua` repl, and send the command over the `RDN:CHATBOX:v5` rednet protocol, e.g.
    ```lua
    peripheral.find("modem", rednet.open)
    rednet.broadcast("rn update", "RDN:CHATBOX:v5")
    ```

## Known Limitations
- Pausing the game can throw off timings and desynchronize the audio. *Use in single player is not recommended*.
- Active clients skip ahead slightly whenever a client joins the session to maintain synchronization.
- Audio may become choppy if no players are in range of a speaker. This will self-correct over time, but you can expedite the process by toggling Quit/Join or using the `rn sync` command. 
- Age-restricted and videos longer than ~20 minutes may fail to download. The exact threshold varies; in testing, the shortest failure was ~18min, the longest success ~45min.

## Acknowledgements
- [Rypo/redionet](https://github.com/Rypo/redionet) — upstream project and sync algorithm this fork is based on.
- The [foundational code author](https://github.com/terreng/computercraft-streaming-music) and backend host. This project is rooted in his original work. 
- [YouCube](https://github.com/CC-YouCube/client) - a continual source of design and function inspiration throughout development.
