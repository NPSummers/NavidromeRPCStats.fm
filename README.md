# Navidrome Discord RPC + Stats.fm

One Python process polls Navidrome, shows the current song as a Spotify-style
Discord presence, records completed listens, and optionally imports them into
Stats.fm. Spotify matching and album art do not require a Spotify access token.

The Discord activity uses Navidrome's reported position to calculate a stable
start time and end time. Seeking recalculates the timestamps. A listen at 70%
or less is discarded and is never added to history or the Stats.fm queue.

> Discord does not permit automated user accounts. A selfbot can result in
> account suspension. Use it at your own risk.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Fill in the single `.env` file. `STATSFM_TOKEN` is optional; leave it blank to
disable Stats.fm uploads. No Spotify token is needed.

Validate and run:

```powershell
python main.py --check
python main.py
```

The generated `spotify_history.json` and its Stats.fm state file stay beside
`main.py` unless `HISTORY_FILE` is changed. After Stats.fm accepts an upload,
the local history is reset to an empty JSON array. Failed uploads retain the
history for retry. The health endpoint listens on port 3000 by default; pass
`--no-health` to disable it.
