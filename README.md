# YouTube Archiver

Headless YouTube playlist archiver: downloads high-quality videos sequentially, retries across multiple extractor profiles, embeds rich metadata + cover art, and copies to your library while tracking history in SQLite. Optional helpers include a Tkinter config editor and a one-time OAuth token generator.

 Use at your own discretion - low volume should not trigger any bans or blacklisting but this is not guaranteed.

 Videos download as webm first attempt, then mp4 - final format (webm, mp4, mkv) can be configured in config.json.

 Telegram messages optional - sends a summary only if there were attempted downloads. Nice feature to have setup!

## What’s here
- `archiver.py` — main downloader (sequential, retries, metadata embed, filename templating, optional Telegram summary).
- `config/config_sample.json` — copy to `config/config.json` and fill in your accounts/playlists.
- `setup_oauth.py` — headless OAuth helper to generate token JSONs.
- `config_gui.py` — optional GUI to edit `config.json` (accounts are read-only; manage token paths on disk).

## Requirements
- Python 3.10+
- ffmpeg on PATH (for metadata/cover art)
- yt-dlp (installed via `requirements.txt`)
- Deno or Node.js runtime (auto-detected, or set `js_runtime`/`YT_DLP_JS_RUNTIME`)
- Google Cloud project with YouTube Data API v3 enabled

## Install
```bash
git clone https://github.com/yourname/youtube-archiver.git
cd youtube-archiver
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```
Install ffmpeg via your package manager (Homebrew `brew install ffmpeg`, Debian/Ubuntu `sudo apt-get install ffmpeg`, etc.).

## Configure
1) Copy the sample: `cp config/config_sample.json config/config.json`
2) Create OAuth client in Google Cloud (Desktop app) and download `client_secret_*.json` into `tokens/`.
3) Generate tokens (one per account):
   ```bash
   python setup_oauth.py --account family_tv tokens/client_secret_family.json tokens/token_family.json
   ```
4) Edit `config/config.json`:
   - `accounts` → paths to client_secret and token JSONs
   - `playlists` → playlist_id, folder, account, optional remove_after_download
   - `filename_template` → Python `%` template with `title`, `uploader`, `upload_date`, `ext`
   - `final_format` → `webm`, `mp4`, or leave blank; this is a container copy only (if a download falls back to MP4, set `final_format` to `mp4` or leave blank)
   - `js_runtime` → optional override (`deno:/path/to/deno` or `node:/path/to/node`)
   - `telegram` → optional bot_token/chat_id for summaries
   - `yt_dlp_opts` → optional extra yt-dlp options to merge

Download order: prefers WebM VP9/Opus (1080p → 720p), then MP4 (1080p → 720p). Metadata embedding keeps the original container; MP4→WebM remux is skipped to avoid broken files.

## Run (headless)
```bash
source .venv/bin/activate
umask 0002                        # if you need group-writable files (e.g., NFS/SMB)
python archiver.py --config config/config.json
```
- Uses `temp_downloads/` for work files, logs to `logs/archiver.log`, history in `database/db.sqlite`, lockfile at `/tmp/yt_archiver.lock`.
- Ensure the user running the archiver owns the repo/logs/db/temp folders and can write to the temp directory (`/tmp/yt-dlp` by default).

### Cron example (run as a non-root user)
```
*/30 * * * * umask 0002; cd /opt/Scripts/youtube-archiver && /opt/Scripts/youtube-archiver/.venv/bin/python3 archiver.py --config /opt/Scripts/youtube-archiver/config/config.json >> /opt/Scripts/youtube-archiver/logs/cron.log 2>&1
```
[I ran in Debian LXC and created a 'media' user with UID 1000, and added this to the media crontab - was required for my setup to get permissions correct!]

### Optional GUI
```bash
python config_gui.py
```
Pick your `config.json`, edit playlists/Telegram/template/final format, and save. Accounts are displayed read-only; manage OAuth files on disk.

## Notes & tips
- Keep `tokens/` and real `config/config.json` out of version control (.gitignore already does).
- If you need a custom temp path, set it via `yt_dlp_opts` → `"paths": { "temp": "/path/you/own" }`.
- ffmpeg is required for metadata embedding and cover art.

## License
MIT — see LICENSE.
