import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle
import pandas as pd
import pytz


class YouTubeLiveViewsFinder:
    def __init__(self):
        """Initialize the YouTube API client."""
        load_dotenv()

        self.SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
        self.youtube = self._authenticate()
        self.channel_id = os.getenv("YOUTUBE_CHANNEL_ID")

        # Assume Eastern Time for the church
        self.local_tz = pytz.timezone("America/New_York")

        # Log file for discrepancies
        self.discrepancy_log = []

    def _authenticate(self):
        """Authenticate with YouTube API using OAuth2."""
        creds = None

        # Token file stores the user's access and refresh tokens
        if os.path.exists("token.pickle"):
            with open("token.pickle", "rb") as token:
                creds = pickle.load(token)

        # If there are no valid credentials, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists("client_secret.json"):
                    raise ValueError(
                        "client_secret.json not found. Please download it from Google Cloud Console."
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    "client_secret.json", self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open("token.pickle", "wb") as token:
                pickle.dump(creds, token)

        return build("youtube", "v3", credentials=creds)

    def get_uploads_playlist_id(self):
        """Get the uploads playlist ID for the channel."""
        try:
            request = self.youtube.channels().list(
                part="contentDetails", id=self.channel_id
            )
            response = request.execute()

            if not response.get("items"):
                print("Channel not found.")
                return None

            uploads_playlist_id = response["items"][0]["contentDetails"][
                "relatedPlaylists"
            ]["uploads"]
            return uploads_playlist_id

        except Exception as e:
            print(f"Error getting uploads playlist: {e}")
            return None

    def get_all_live_streams(self, max_results=2000, start_date=None, end_date=None):
        """Fetch all live stream videos from the channel (including unlisted)."""
        if not self.channel_id:
            raise ValueError("Channel ID not set")

        # Get the uploads playlist ID
        uploads_playlist_id = self.get_uploads_playlist_id()
        if not uploads_playlist_id:
            print("Could not find uploads playlist")
            return []

        print(f"Fetching videos from uploads playlist: {uploads_playlist_id}")
        if start_date and end_date:
            print(f"Filtering videos from {start_date} to {end_date}")

        videos = []
        next_page_token = None

        while len(videos) < max_results:
            try:
                # Get playlist items (this includes unlisted videos when authenticated)
                request = self.youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=min(50, max_results - len(videos)),
                    pageToken=next_page_token,
                )
                response = request.execute()

                if not response.get("items"):
                    break

                video_ids = [
                    item["contentDetails"]["videoId"] for item in response["items"]
                ]

                # Get detailed video information with optional date filtering
                videos_request = self.youtube.videos().list(
                    part="snippet,statistics,liveStreamingDetails,status",
                    id=",".join(video_ids),
                )

                # Add date filtering if provided
                if start_date:
                    videos_request = videos_request.filter(
                        publishedAfter=start_date.isoformat() + "Z"
                    )
                if end_date:
                    videos_request = videos_request.filter(
                        publishedBefore=end_date.isoformat() + "Z"
                    )

                videos_response = videos_request.execute()

                for video in videos_response.get("items", []):
                    live_details = video.get("liveStreamingDetails", {})

                    # Only include videos that have live streaming details
                    if live_details.get("actualStartTime"):
                        # Apply date filtering if specified
                        if start_date or end_date:
                            published_str = video["snippet"]["publishedAt"]
                            published_date = datetime.fromisoformat(
                                published_str.replace("Z", "+00:00")
                            ).date()

                            if start_date and published_date < start_date:
                                continue
                            if end_date and published_date > end_date:
                                continue

                        video_info = {
                            "id": video["id"],
                            "title": video["snippet"]["title"],
                            "published": video["snippet"]["publishedAt"],
                            "status": video["status"]["privacyStatus"],
                            "views": video["statistics"].get("viewCount", "0"),
                            "live_start": live_details.get("actualStartTime"),
                            "live_end": live_details.get("actualEndTime"),
                        }
                        videos.append(video_info)

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

                print(f"Fetched {len(videos)} live streams so far...")

            except Exception as e:
                print(f"Error fetching videos: {e}")
                break

        print(f"Total live streams found: {len(videos)}")
        return videos

    def utc_to_local(self, utc_time_str):
        """Convert UTC time string to local timezone."""
        utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
        utc_time = pytz.utc.localize(utc_time)
        local_time = utc_time.astimezone(self.local_tz)
        return local_time

    def get_duration_hours(self, start_str, end_str):
        """Calculate duration in hours between two time strings."""
        if not start_str or not end_str:
            return 0

        start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%SZ")
        duration = (end - start).total_seconds() / 3600
        return duration

    def find_stream_for_date(self, target_date, all_streams):
        """
        Find the best matching stream(s) for a given date.

        Returns:
            dict: {'9am': view_count or None, '10:45am': view_count or None, 'notes': str}
        """
        result = {"9am": None, "10:45am": None, "notes": ""}

        # Filter streams for the target date
        matching_streams = []

        for stream in all_streams:
            if not stream.get("live_start"):
                continue

            # Convert to local time
            local_start = self.utc_to_local(stream["live_start"])
            stream_date = local_start.date()

            # Check if it's the target date
            if stream_date != target_date:
                continue

            # Filter to 7:00 AM - 1:00 PM local time
            start_hour = local_start.hour + local_start.minute / 60
            if start_hour < 7 or start_hour > 13:
                continue

            # Calculate duration
            duration = self.get_duration_hours(stream["live_start"], stream["live_end"])

            matching_streams.append(
                {
                    "stream": stream,
                    "local_start": local_start,
                    "duration": duration,
                    "start_hour": start_hour,
                }
            )

        if not matching_streams:
            result["notes"] = "No streams found"
            return result

        # Remove duplicate streams (same video ID appearing multiple times)
        seen_ids = set()
        unique_streams = []
        for ms in matching_streams:
            video_id = ms["stream"]["id"]
            if video_id not in seen_ids:
                seen_ids.add(video_id)
                unique_streams.append(ms)

        matching_streams = unique_streams

        # Log if multiple streams found
        if len(matching_streams) > 1:
            log_entry = f"WARNING: {target_date}: Found {len(matching_streams)} streams"
            for ms in matching_streams:
                log_entry += f"\n    - {ms['local_start'].strftime('%I:%M %p')} | Duration: {ms['duration']:.1f}h | Views: {ms['stream']['views']}"
            self.discrepancy_log.append(log_entry)
            print(f"WARNING: {target_date}: Found {len(matching_streams)} streams")

        # Check if there's a combined stream (> 2.5 hours)
        combined_stream = None
        for ms in matching_streams:
            if ms["duration"] > 2.5:
                combined_stream = ms
                break

        if combined_stream:
            # Combined stream - record only for 9am service
            result["9am"] = combined_stream["stream"]["views"]
            result["10:45am"] = 0
            result["notes"] = (
                f"Combined stream ({combined_stream['duration']:.1f}h), recorded for 9am only"
            )
            print(
                f"  > Combined stream ({combined_stream['duration']:.1f}h), recorded for 9am only"
            )
            return result

        # No combined stream - look for separate 9am and 10:45am services
        service_9am = None
        service_1045am = None

        for ms in matching_streams:
            start_hour = ms["start_hour"]

            print(
                f"  DEBUG: Stream at {ms['local_start'].strftime('%I:%M %p')} (hour={start_hour:.2f})"
            )

            # 9am service: starts between 8:00-10:00 AM
            if 8 <= start_hour < 10:
                print(f"    -> Matched as 9am service")
                if service_9am is None or ms["duration"] > service_9am["duration"]:
                    service_9am = ms

            # 10:45am service: starts between 10:00 AM-12:00 PM
            elif 10 <= start_hour <= 12:
                print(f"    -> Matched as 10:45am service")
                if (
                    service_1045am is None
                    or ms["duration"] > service_1045am["duration"]
                ):
                    service_1045am = ms
            else:
                print(f"    -> NOT matched (outside time windows)")

        # Record the services found
        notes_parts = []

        if service_9am:
            result["9am"] = service_9am["stream"]["views"]
            notes_parts.append(f"9am: {service_9am['duration']:.1f}h")
            print(
                f"  > 9am service ({service_9am['duration']:.1f}h) - {service_9am['stream']['views']} views"
            )

        if service_1045am:
            result["10:45am"] = service_1045am["stream"]["views"]
            notes_parts.append(f"10:45am: {service_1045am['duration']:.1f}h")
            print(
                f"  > 10:45am service ({service_1045am['duration']:.1f}h) - {service_1045am['stream']['views']} views"
            )

        if notes_parts:
            result["notes"] = ", ".join(notes_parts)
        else:
            # No clear match
            best = matching_streams[0]
            result["notes"] = (
                f"Unclear time: {best['local_start'].strftime('%I:%M %p')} ({best['duration']:.1f}h)"
            )
            print(
                f"  > Unclear time: {best['local_start'].strftime('%I:%M %p')} ({best['duration']:.1f}h)"
            )

        return result

    def process_attendance_csv(self, csv_file="attendance.csv", output_file=None):
        """
        Process the attendance CSV and populate YouTube view counts.

        Args:
            csv_file (str): Input CSV file path
            output_file (str): Output CSV file path (optional)
        """
        if not output_file:
            output_file = csv_file.replace(".csv", "_with_youtube.csv")

        # Read the CSV file
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return

        print(f"Processing {len(df)} attendance records...")
        print(f"Columns: {list(df.columns)}")

        # Ensure YouTube columns exist
        if "youtube 9am" not in df.columns:
            df["youtube 9am"] = ""
        if "youtube 1045am" not in df.columns:
            df["youtube 1045am"] = ""
        if "youtube notes" not in df.columns:
            df["youtube notes"] = ""

        # Fetch all live streams once
        print("\nFetching all live streams from YouTube...")
        all_streams = self.get_all_live_streams()

        if not all_streams:
            print("No live streams found. Exiting.")
            return

        # Save raw data for reference
        with open("all_streams.json", "w") as f:
            json.dump(all_streams, f, indent=2)
        print(f"Saved raw stream data to all_streams.json")

        processed_count = 0
        updated_count = 0

        for index, row in df.iterrows():
            try:
                # Parse the date
                date_str = str(row["date"]).strip()

                # Handle different date formats
                for date_format in ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
                    try:
                        target_date = datetime.strptime(date_str, date_format).date()
                        break
                    except ValueError:
                        continue
                else:
                    print(
                        f"Skipping row {index + 1}: Could not parse date '{date_str}'"
                    )
                    continue

                processed_count += 1

                # Skip if both YouTube columns already have data
                if (
                    pd.notna(row.get("youtube 9am"))
                    and str(row.get("youtube 9am")).strip()
                    and pd.notna(row.get("youtube 1045am"))
                    and str(row.get("youtube 1045am")).strip()
                ):
                    print(f"Skipping {target_date}: YouTube data already exists")
                    continue

                print(f"\nProcessing {target_date} (row {index + 1})...")

                # Find matching stream(s)
                result = self.find_stream_for_date(target_date, all_streams)

                # Update the dataframe
                # Update the dataframe
                row_updated = False

                if result["9am"] is not None:
                    df.at[index, "youtube 9am"] = str(int(result["9am"]))
                    row_updated = True

                if result["10:45am"] is not None:
                    df.at[index, "youtube 1045am"] = str(int(result["10:45am"]))
                    row_updated = True

                if result["notes"]:
                    df.at[index, "youtube notes"] = result["notes"]

                if row_updated:
                    updated_count += 1

            except Exception as e:
                print(f"Error processing row {index + 1}: {e}")
                continue

        # Save the updated CSV with UTF-8 encoding
        try:
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"\n{'='*60}")
            print(f"SUCCESS: Processed {processed_count} rows")
            print(f"SUCCESS: Updated {updated_count} rows with YouTube data")
            print(f"SUCCESS: Saved to: {output_file}")

            # Save discrepancy log
            if self.discrepancy_log:
                log_file = "youtube_discrepancies.log"
                with open(log_file, "w", encoding="utf-8") as f:
                    f.write("\n\n".join(self.discrepancy_log))
                print(
                    f"WARNING: Logged {len(self.discrepancy_log)} discrepancies to: {log_file}"
                )

        except Exception as e:
            print(f"Error saving CSV file: {e}")

    def process_date_range(self, start_date, end_date, output_file=None):
        """Process live streams within a date range and generate statistics."""
        if not output_file:
            output_file = f'youtube_stats_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'

        print(f"Processing YouTube live streams from {start_date} to {end_date}")

        # Fetch live streams in date range
        all_streams = self.get_all_live_streams(
            start_date=start_date, end_date=end_date
        )

        if not all_streams:
            print("No live streams found in date range.")
            return

        # Group streams by date
        date_groups = {}
        for stream in all_streams:
            start_str = stream.get("live_start")
            if not start_str:
                continue

            local_time = self.utc_to_local(start_str)
            if not local_time:
                continue

            stream_date = local_time.date()

            # Skip if outside our range
            if stream_date < start_date or stream_date > end_date:
                continue

            if stream_date not in date_groups:
                date_groups[stream_date] = []

            # Calculate duration
            end_str = stream.get("live_end")
            if end_str:
                end_time = self.utc_to_local(end_str)
                duration = (end_time - local_time).total_seconds() / 3600  # hours
            else:
                duration = 0

            # Filter out very short streams (less than 30 minutes)
            if duration < 0.5:
                continue

            date_groups[stream_date].append(
                {
                    "stream": stream,
                    "local_start": local_time,
                    "duration": duration,
                    "start_hour": local_time.hour + local_time.minute / 60,
                    "views": int(stream.get("views", 0)),
                }
            )

        # Process each date
        results = []
        for date in sorted(date_groups.keys()):
            streams = date_groups[date]

            result = {
                "date": date.strftime("%m/%d/%Y"),
                "youtube_9am": None,
                "youtube_1045am": None,
                "youtube_notes": "",
            }

            # Apply the same logic as find_stream_for_date
            if len(streams) == 2:
                # Sort by start time
                streams.sort(key=lambda x: x["start_hour"])
                result["youtube_9am"] = streams[0]["views"]
                result["youtube_1045am"] = streams[1]["views"]
                result["youtube_notes"] = "Two streams found"
            elif len(streams) == 1:
                stream = streams[0]
                if stream["duration"] > 2.5:
                    # Long stream - counts for both services
                    result["youtube_9am"] = stream["views"]
                    result["youtube_1045am"] = stream["views"]
                    result["youtube_notes"] = f'Long stream ({stream["duration"]:.1f}h)'
                else:
                    # Short stream - guess based on time
                    if 8 <= stream["start_hour"] <= 10:
                        result["youtube_9am"] = stream["views"]
                        result["youtube_notes"] = (
                            f'Short stream at {stream["start_hour"]:.1f}h'
                        )
                    elif 10 <= stream["start_hour"] <= 12:
                        result["youtube_1045am"] = stream["views"]
                        result["youtube_notes"] = (
                            f'Short stream at {stream["start_hour"]:.1f}h'
                        )
                    else:
                        result["youtube_notes"] = (
                            f'Stream at {stream["start_hour"]:.1f}h (unclear service)'
                        )
            else:
                result["youtube_notes"] = f"{len(streams)} streams found"

            results.append(result)

        # Save to CSV
        if results:
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"✅ Processed {len(results)} dates, saved to {output_file}")
        else:
            print("No results to save.")

        return results


def main():
    """Main function with interactive setup."""
    print("YouTube Live Views Finder")
    print("=" * 40)

    try:
        finder = YouTubeLiveViewsFinder()
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("\nPlease make sure you have authenticated with OAuth2")
        return

    # Check if channel ID is set
    if not finder.channel_id:
        print("\nChannel ID not configured.")
        manual_id = input("Enter your YouTube channel ID: ").strip()
        if manual_id:
            finder.channel_id = manual_id
            print(f"Using channel ID: {manual_id}")
        else:
            print("No channel ID configured. Exiting.")
            return

    print("✅ YouTube API connection successful")
    print(
        "Use YouTubeLiveViewsFinder.process_date_range(start_date, end_date) to fetch videos"
    )


if __name__ == "__main__":
    main()
