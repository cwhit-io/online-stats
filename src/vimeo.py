import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import pandas as pd
import pytz

class VimeoLiveViewsFinder:
    def __init__(self):
        """Initialize the Vimeo API client."""
        load_dotenv()
        
        self.access_token = os.getenv('VIMEO_ACCESS_TOKEN')
        self.user_id = os.getenv('VIMEO_USER_ID')
        
        if not self.access_token:
            raise ValueError("VIMEO_ACCESS_TOKEN not found in .env file")
        
        self.base_url = 'https://api.vimeo.com'
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/vnd.vimeo.*+json;version=3.4'
        }
        
        self.local_tz = pytz.timezone('America/New_York')
        self.discrepancy_log = []
    
    def get_all_videos(self, max_results=2000):
        """Fetch all videos from the Vimeo account."""
        if not self.user_id:
            response = requests.get(f'{self.base_url}/me', headers=self.headers)
            if response.status_code == 200:
                user_data = response.json()
                self.user_id = user_data['uri'].split('/')[-1]
                print(f"Authenticated as: {user_data['name']} (ID: {self.user_id})")
            else:
                raise ValueError(f"Failed to authenticate: {response.status_code} - {response.text}")
        
        print(f"Fetching videos from Vimeo user: {self.user_id}")
        
        videos = []
        page = 1
        per_page = 100
        
        while len(videos) < max_results:
            try:
                url = f'{self.base_url}/users/{self.user_id}/videos'
                params = {
                    'page': page,
                    'per_page': min(per_page, max_results - len(videos)),
                    'fields': 'uri,name,created_time,stats,privacy,duration,modified_time,release_time,type',
                    'sort': 'date',
                    'direction': 'desc'
                }
                
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                if response.status_code != 200:
                    print(f"Error fetching videos: {response.status_code}")
                    print(response.text)
                    break
                
                data = response.json()
                page_videos = data.get('data', [])
                
                if not page_videos:
                    break
                
                for video in page_videos:
                    video_info = {
                        'id': video['uri'].split('/')[-1],
                        'title': video['name'],
                        'created': video['created_time'],
                        'modified': video.get('modified_time'),
                        'release_time': video.get('release_time'),
                        'privacy': video['privacy']['view'],
                        'views': video['stats']['plays'],
                        'duration': video.get('duration', 0),
                        'type': video.get('type', 'video')
                    }
                    videos.append(video_info)
                
                print(f"Fetched {len(videos)} total videos so far... (page {page})")
                
                paging = data.get('paging', {})
                if not paging.get('next'):
                    break
                
                page += 1
                
            except requests.exceptions.Timeout:
                print(f"Timeout on page {page}, retrying...")
                continue
            except Exception as e:
                print(f"Error fetching videos on page {page}: {e}")
                break
        
        print(f"\\nTotal videos found: {len(videos)}")
        return videos
    
    def parse_vimeo_time(self, time_str):
        """Parse Vimeo ISO 8601 time string to datetime."""
        if not time_str:
            return None
        
        try:
            if time_str.endswith('Z'):
                dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')
                dt = pytz.utc.localize(dt)
            elif '+' in time_str or time_str.count('-') > 2:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(time_str)
                dt = pytz.utc.localize(dt)
            
            return dt
        except Exception as e:
            print(f"Error parsing time '{time_str}': {e}")
            return None
    
    def utc_to_local(self, utc_time):
        """Convert UTC datetime to local timezone."""
        if isinstance(utc_time, str):
            utc_time = self.parse_vimeo_time(utc_time)
        
        if not utc_time:
            return None
        
        if utc_time.tzinfo is None:
            utc_time = pytz.utc.localize(utc_time)
        
        local_time = utc_time.astimezone(self.local_tz)
        return local_time
    
    def get_duration_hours(self, duration_seconds):
        """Convert duration in seconds to hours."""
        if not duration_seconds:
            return 0
        return duration_seconds / 3600
    
    def find_stream_for_date(self, target_date, all_videos):
        """
        Find the best matching video(s) for a given date.
        
        NEW LOGIC:
        - Look for videos created/uploaded on the target date (regardless of time)
        - If 2 videos found: earlier one = 9am, later one = 10:45am
        - If 1 video found and duration > 2.5h: combined stream (record for both)
        - If 1 video found and duration < 2.5h: try to guess based on upload time
        
        Returns:
            dict: {'9am': view_count or None, '10:45am': view_count or None, 'notes': str}
        """
        result = {
            '9am': None,
            '10:45am': None,
            'notes': ''
        }
        
        # Filter videos for the target date (ANY time of day)
        matching_videos = []
        
        for video in all_videos:
            time_str = video.get('created')
            if not time_str:
                continue
            
            local_time = self.utc_to_local(time_str)
            if not local_time:
                continue
            
            video_date = local_time.date()
            
            # Check if it's the target date
            if video_date != target_date:
                continue
            
            # Calculate duration
            duration = self.get_duration_hours(video.get('duration', 0))
            
            # Filter out very short videos (less than 30 minutes)
            if duration < 0.5:
                continue
            
            matching_videos.append({
                'video': video,
                'local_start': local_time,
                'duration': duration,
                'start_hour': local_time.hour + local_time.minute / 60
            })
        
        if not matching_videos:
            result['notes'] = 'No videos found'
            return result
        
        # Remove duplicates
        seen_ids = set()
        unique_videos = []
        for mv in matching_videos:
            video_id = mv['video']['id']
            if video_id not in seen_ids:
                seen_ids.add(video_id)
                unique_videos.append(mv)
        
        matching_videos = sorted(unique_videos, key=lambda x: x['local_start'])
        
        # Log if multiple videos found
        if len(matching_videos) > 1:
            log_entry = f"INFO: {target_date}: Found {len(matching_videos)} videos"
            for mv in matching_videos:
                log_entry += f"\\n  - {mv['local_start'].strftime('%I:%M %p')} | {mv['duration']:.1f}h | {mv['video']['views']} views"
            self.discrepancy_log.append(log_entry)
            print(f"INFO: {target_date}: Found {len(matching_videos)} videos")
        
        # CASE 1: Single combined video (> 2.5 hours)
        if len(matching_videos) == 1 and matching_videos[0]['duration'] > 2.5:
            mv = matching_videos[0]
            result['9am'] = mv['video']['views']
            result['10:45am'] = 0
            result['notes'] = f"Combined video ({mv['duration']:.1f}h) at {mv['local_start'].strftime('%I:%M %p')}"
            print(f"  ✓ Combined video ({mv['duration']:.1f}h) - {mv['video']['views']} views (recorded for both services)")
            return result
        
        # CASE 2: Two separate videos
        if len(matching_videos) == 2:
            earlier = matching_videos[0]
            later = matching_videos[1]
            
            result['9am'] = earlier['video']['views']
            result['10:45am'] = later['video']['views']
            result['notes'] = f"9am: {earlier['local_start'].strftime('%I:%M %p')} ({earlier['duration']:.1f}h), 10:45am: {later['local_start'].strftime('%I:%M %p')} ({later['duration']:.1f}h)"
            
            print(f"  ✓ 9am service: {earlier['local_start'].strftime('%I:%M %p')} ({earlier['duration']:.1f}h) - {earlier['video']['views']} views")
            print(f"  ✓ 10:45am service: {later['local_start'].strftime('%I:%M %p')} ({later['duration']:.1f}h) - {later['video']['views']} views")
            return result
        
        # CASE 3: Single video (not combined)
        if len(matching_videos) == 1:
            mv = matching_videos[0]
            start_hour = mv['start_hour']
            
            # Try to guess which service based on upload time
            if start_hour < 11:
                # Uploaded before 11am - likely 9am service
                result['9am'] = mv['video']['views']
                result['notes'] = f"Single video at {mv['local_start'].strftime('%I:%M %p')} ({mv['duration']:.1f}h) - assumed 9am"
                print(f"  ✓ 9am service (assumed): {mv['local_start'].strftime('%I:%M %p')} ({mv['duration']:.1f}h) - {mv['video']['views']} views")
            else:
                # Uploaded after 11am - likely 10:45am service
                result['10:45am'] = mv['video']['views']
                result['notes'] = f"Single video at {mv['local_start'].strftime('%I:%M %p')} ({mv['duration']:.1f}h) - assumed 10:45am"
                print(f"  ✓ 10:45am service (assumed): {mv['local_start'].strftime('%I:%M %p')} ({mv['duration']:.1f}h) - {mv['video']['views']} views")
            
            return result
        
        # CASE 4: More than 2 videos - use heuristics
        if len(matching_videos) > 2:
            # Sort by upload time
            sorted_videos = sorted(matching_videos, key=lambda x: x['local_start'])
            
            # Take first two as 9am and 10:45am
            result['9am'] = sorted_videos[0]['video']['views']
            result['10:45am'] = sorted_videos[1]['video']['views']
            result['notes'] = f"Multiple videos ({len(matching_videos)}), used first two"
            
            print(f"  ⚠ Multiple videos found - using first two:")
            print(f"    9am: {sorted_videos[0]['local_start'].strftime('%I:%M %p')} - {sorted_videos[0]['video']['views']} views")
            print(f"    10:45am: {sorted_videos[1]['local_start'].strftime('%I:%M %p')} - {sorted_videos[1]['video']['views']} views")
            
            return result
        
        return result
    
    def process_attendance_csv(self, csv_file='attendance_with_youtube.csv', output_file=None):
        """Process the attendance CSV and populate Vimeo view counts."""
        if not output_file:
            output_file = csv_file.replace('.csv', '_with_vimeo.csv')
        
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return
        
        print(f"Processing {len(df)} attendance records...")
        
        # Ensure Vimeo columns exist
        if 'vimeo 9am' not in df.columns:
            df['vimeo 9am'] = ''
        if 'vimeo 1045am' not in df.columns:
            df['vimeo 1045am'] = ''
        if 'vimeo notes' not in df.columns:
            df['vimeo notes'] = ''
        
        # Fetch all videos once
        print("\\nFetching all videos from Vimeo...")
        all_videos = self.get_all_videos()
        
        if not all_videos:
            print("No videos found. Exiting.")
            return
        
        # Save raw data
        with open('all_vimeo_videos.json', 'w') as f:
            json.dump(all_videos, f, indent=2)
        print(f"Saved raw video data to all_vimeo_videos.json\\n")
        
        processed_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            try:
                date_str = str(row['date']).strip()
                
                for date_format in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
                    try:
                        target_date = datetime.strptime(date_str, date_format).date()
                        break
                    except ValueError:
                        continue
                else:
                    print(f"Skipping row {index + 1}: Could not parse date '{date_str}'")
                    continue
                
                processed_count += 1
                
                # Skip if both columns already have data
                if (pd.notna(row.get('vimeo 9am')) and str(row.get('vimeo 9am')).strip() and 
                    pd.notna(row.get('vimeo 1045am')) and str(row.get('vimeo 1045am')).strip()):
                    print(f"Skipping {target_date}: Vimeo data already exists")
                    continue
                
                print(f"\\nProcessing {target_date} (row {index + 1})...")
                
                # Find matching video(s)
                result = self.find_stream_for_date(target_date, all_videos)
                
                # Update the dataframe
                # Update the dataframe
                row_updated = False

                if result['9am'] is not None:
                    df.at[index, 'vimeo 9am'] = str(int(result['9am']))
                    row_updated = True

                if result['10:45am'] is not None:
                    df.at[index, 'vimeo 1045am'] = str(int(result['10:45am']))
                    row_updated = True

                if result['notes']:
                    df.at[index, 'vimeo notes'] = result['notes']

                if row_updated:
                    updated_count += 1

                
            except Exception as e:
                print(f"Error processing row {index + 1}: {e}")
                continue
        
        # Save the updated CSV
        try:
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\\n{'='*60}")
            print(f"SUCCESS: Processed {processed_count} rows")
            print(f"SUCCESS: Updated {updated_count} rows with Vimeo data")
            print(f"SUCCESS: Saved to: {output_file}")
            
            if self.discrepancy_log:
                log_file = 'vimeo_discrepancies.log'
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write('\\n\\n'.join(self.discrepancy_log))
                print(f"INFO: Logged {len(self.discrepancy_log)} notes to: {log_file}")
            
        except Exception as e:
            print(f"Error saving CSV file: {e}")

def main():
    """Main function."""
    print("Vimeo Live Views Finder - UPDATED VERSION")
    print("=" * 40)
    
    try:
        finder = VimeoLiveViewsFinder()
    except ValueError as e:
        print(f"Configuration error: {e}")
        return
    
    finder.process_attendance_csv()

if __name__ == "__main__":
    main()