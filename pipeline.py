import os
import json
import requests
from dotenv import load_dotenv
from googleapiclient.discovery import build
import psycopg2
from psycopg2.extras import RealDictCursor
from google import genai
from google.genai import types

load_dotenv()

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
DATABASE_URL = os.environ.get("SUPABASE_DB_URL")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def run_pipeline():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM channels;")
    cohort = cur.fetchall()
    if not cohort:
        print("No channels found in master table.")
        return

    channel_map = {c["channel_id"]: c for c in cohort}
    channel_ids = list(channel_map.keys())

    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    yt_request = youtube.channels().list(
        part="statistics,contentDetails",
        id=",".join(channel_ids)
    )
    yt_response = yt_request.execute()

    for item in yt_response.get("items", []):
        cid = item["id"]
        meta = channel_map[cid]
        stats = item.get("statistics", {})
        content_details = item.get("contentDetails", {})

        uploads_playlist = content_details.get("relatedPlaylists", {}).get("uploads")
        if uploads_playlist and not meta["uploads_playlist_id"]:
            cur.execute(
                "UPDATE channels SET uploads_playlist_id = %s WHERE channel_id = %s",
                (uploads_playlist, cid)
            )
            meta["uploads_playlist_id"] = uploads_playlist

        total_views = int(stats.get("viewCount", 0))
        subs = int(stats.get("subscriberCount", 0))
        vids = int(stats.get("videoCount", 0))

        lifetime_yield = (total_views / 1000) * float(meta["category_rpm"])

        cur.execute(
            """
            INSERT INTO channel_metrics_snapshots 
            (channel_id, subscriber_count, total_lifetime_views, video_count, est_platform_lifetime_yield)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (cid, subs, total_views, vids, lifetime_yield)
        )
        print(f"Logged snapshot for {meta['name']}: {total_views:,} views (${lifetime_yield:,.2f} est. yield)")

        if meta.get("uploads_playlist_id"):
            check_recent_sponsorships(youtube, cur, cid, meta["uploads_playlist_id"])

    conn.commit()
    cur.close()
    conn.close()
    print("Pipeline run completed successfully.")

def check_recent_sponsorships(youtube, cur, channel_id, uploads_playlist_id):
    playlist_req = youtube.playlistItems().list(
        part="snippet",
        playlistId=uploads_playlist_id,
        maxResults=3
    )
    res = playlist_req.execute()

    for item in res.get("items", []):
        video_id = item["snippet"]["resourceId"]["videoId"]
        title = item["snippet"]["title"]
        published_at = item["snippet"]["publishedAt"]

        cur.execute("SELECT id FROM sponsorships WHERE video_id = %s", (video_id,))
        if cur.fetchone():
            continue

        sb_url = f"https://sponsor.ajay.app/api/skipSegments?videoID={video_id}&categories=[\"sponsor\"]"
        sb_res = requests.get(sb_url)

        if sb_res.status_code == 200:
            segments = sb_res.json()
            if segments:
                extract_and_save_sponsor(cur, channel_id, video_id, title, published_at)

def extract_and_save_sponsor(cur, channel_id, video_id, video_title, published_at):
    if not GEMINI_API_KEY:
        return

    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = f"""Identify the sponsored brand in this YouTube video from its title:
Title: "{video_title}"
Video ID: "{video_id}"

Return JSON matching:
{{"brand_name": string, "sponsor_category": string}}
If unknown or not identifiable, set brand_name to "Undisclosed Sponsor"."""

    try:
        response = client.models.generate_content(
            model="gemini-3.8-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        parsed = json.loads(response.text)
        brand = parsed.get("brand_name", "Undisclosed Sponsor")
        cat = parsed.get("sponsor_category", "General")

        cur.execute(
            """
            INSERT INTO sponsorships (video_id, channel_id, brand_name, sponsor_category, video_published_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_id, brand_name) DO NOTHING;
            """,
            (video_id, channel_id, brand, cat, published_at)
        )
        print(f"Detected sponsor '{brand}' ({cat}) on video {video_id}")
    except Exception as e:
        print(f"Failed to parse sponsor for video {video_id}: {e}")

if __name__ == "__main__":
    run_pipeline()
