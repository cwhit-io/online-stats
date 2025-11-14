#!/usr/bin/env python3
"""
Main script to run Vimeo and YouTube analytics and publish results to database.
"""

import os
import sys
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Import the analytics classes
from vimeo import VimeoLiveViewsFinder
from youtube import YouTubeLiveViewsFinder

# Load environment variables
load_dotenv()


class OnlineStatsPublisher:
    """Main class to orchestrate analytics processing and database publishing."""

    def __init__(self):
        """Initialize with database connection."""
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }

        # Validate database configuration
        missing = [k for k, v in self.db_config.items() if not v]
        if missing:
            raise ValueError(f"Missing database configuration: {', '.join(missing)}")

    def get_db_connection(self):
        """Get database connection."""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

    def run_analytics(self, input_csv="attendance.csv"):
        """Run both Vimeo and YouTube analytics."""
        print("Starting Online Video Statistics Processing")
        print("=" * 50)

        # Check if input CSV exists
        if not os.path.exists(input_csv):
            print(f"❌ Input CSV file not found: {input_csv}")
            print(
                "Please ensure your attendance CSV file is in the project root or data/ directory."
            )
            raise FileNotFoundError(f"Input CSV file not found: {input_csv}")

        try:
            # Step 1: Run YouTube analytics
            print("\n1. Running YouTube Analytics...")
            youtube_finder = YouTubeLiveViewsFinder()
            youtube_finder.process_attendance_csv(input_csv)

            # Step 2: Run Vimeo analytics (uses the YouTube-processed file)
            youtube_output = input_csv.replace(".csv", "_with_youtube.csv")
            print(f"\n2. Running Vimeo Analytics on: {youtube_output}")
            vimeo_finder = VimeoLiveViewsFinder()
            vimeo_finder.process_attendance_csv(youtube_output)

            print("\n✅ Analytics processing completed successfully!")

        except Exception as e:
            print(f"❌ Error during analytics processing: {e}")
            raise

    def extract_latest_stats(self):
        """Extract the latest statistics from the processed CSV."""
        csv_file = "attendance_with_vimeo.csv"  # Final output from Vimeo processing

        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Processed CSV file not found: {csv_file}")

        try:
            df = pd.read_csv(csv_file)

            # Get the most recent row (assuming sorted by date)
            if len(df) == 0:
                raise ValueError("No data found in CSV file")

            # Sort by date to get the latest
            df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date_parsed", ascending=False)
            latest_row = df.iloc[0]

            # Extract the required columns
            stats = {
                "youtube_9am": self._extract_numeric_value(
                    latest_row.get("youtube 9am")
                ),
                "vimeo_1045am": self._extract_numeric_value(
                    latest_row.get("vimeo 1045am")
                ),
                "vimeo_9am": self._extract_numeric_value(latest_row.get("vimeo 9am")),
                "youtube_1045am": self._extract_numeric_value(
                    latest_row.get("youtube 1045am")
                ),
            }

            print(f"Extracted stats for date: {latest_row.get('date')}")
            print(f"Stats: {stats}")

            return stats

        except Exception as e:
            print(f"Error extracting stats from CSV: {e}")
            raise

    def _extract_numeric_value(self, value):
        """Extract numeric value from various formats."""
        if pd.isna(value) or value == "" or str(value).strip() == "":
            return None

        try:
            # Convert to string first, then to int
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            print(f"Warning: Could not convert '{value}' to numeric")
            return None

    def publish_to_database(self, stats, dry_run=False):
        """Publish statistics to the database."""
        if dry_run:
            print("🔍 DRY RUN: Would publish the following statistics to database:")
            print(f"   YouTube 9AM: {stats['youtube_9am']}")
            print(f"   Vimeo 10:45AM: {stats['vimeo_1045am']}")
            print(f"   Vimeo 9AM: {stats['vimeo_9am']}")
            print(f"   YouTube 10:45AM: {stats['youtube_1045am']}")
            print(f"   Created At: {datetime.now()}")
            print("✅ Dry run completed - no database changes made!")
            return

        conn = None
        cursor = None

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # Insert or update the online_stats table
            # Assuming we want to insert a new row each time
            insert_query = """
            INSERT INTO online_stats (youtube_9am, vimeo_1045am, vimeo_9am, youtube_1045am, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """

            created_at = datetime.now()

            cursor.execute(
                insert_query,
                (
                    stats["youtube_9am"],
                    stats["vimeo_1045am"],
                    stats["vimeo_9am"],
                    stats["youtube_1045am"],
                    created_at,
                ),
            )

            conn.commit()

            print("✅ Successfully published statistics to database!")
            print(f"   YouTube 9AM: {stats['youtube_9am']}")
            print(f"   Vimeo 10:45AM: {stats['vimeo_1045am']}")
            print(f"   Vimeo 9AM: {stats['vimeo_9am']}")
            print(f"   YouTube 10:45AM: {stats['youtube_1045am']}")

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Error publishing to database: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def run_complete_process(self, input_csv="attendance.csv", dry_run=False):
        """Run the complete process: analytics + database publishing."""
        if dry_run:
            print("🔍 DRY RUN MODE: No actual changes will be made to the database")
            print("=" * 60)

        try:
            # Step 1: Run analytics
            self.run_analytics(input_csv)

            # Step 2: Extract latest stats
            stats = self.extract_latest_stats()

            # Step 3: Publish to database (or simulate in dry-run mode)
            self.publish_to_database(stats, dry_run=dry_run)

            if dry_run:
                print("\n🎭 Dry run completed successfully - no database changes made!")
            else:
                print("\n🎉 Complete process finished successfully!")

        except Exception as e:
            print(f"\n💥 Process failed: {e}")
            sys.exit(1)

    def publish_from_csv(self, csv_file="attendance_with_vimeo.csv", dry_run=False):
        """Extract statistics from processed CSV and publish to database."""
        print("Publishing Statistics to Database")
        print("=" * 40)

        try:
            # Step 1: Extract latest stats from CSV
            stats = self.extract_stats_from_csv(csv_file)

            # Step 2: Publish to database (or simulate in dry-run mode)
            self.publish_to_database(stats, dry_run=dry_run)

            if dry_run:
                print("\n🎭 Dry run completed successfully - no database changes made!")
            else:
                print("\n🎉 Database update completed successfully!")

        except Exception as e:
            print(f"\n💥 Process failed: {e}")
            sys.exit(1)

    def extract_stats_from_csv(self, csv_file):
        """Extract the latest statistics from a specific CSV file."""
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        try:
            df = pd.read_csv(csv_file)

            # Get the most recent row (assuming sorted by date)
            if len(df) == 0:
                raise ValueError("No data found in CSV file")

            # Sort by date to get the latest
            df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date_parsed", ascending=False)
            latest_row = df.iloc[0]

            # Extract the required columns
            stats = {
                "youtube_9am": self._extract_numeric_value(
                    latest_row.get("youtube 9am")
                ),
                "vimeo_1045am": self._extract_numeric_value(
                    latest_row.get("vimeo 1045am")
                ),
                "vimeo_9am": self._extract_numeric_value(latest_row.get("vimeo 9am")),
                "youtube_1045am": self._extract_numeric_value(
                    latest_row.get("youtube 1045am")
                ),
            }

            print(f"Extracted stats for date: {latest_row.get('date')}")
            print(f"Stats: {stats}")

            return stats

        except Exception as e:
            print(f"Error extracting stats from CSV: {e}")
            raise


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Publish online video statistics to database"
    )
    parser.add_argument(
        "--csv",
        default="attendance_with_vimeo.csv",
        help="CSV file to read statistics from (default: attendance_with_vimeo.csv)",
    )
    parser.add_argument(
        "--process",
        action="store_true",
        help="Run analytics processing before publishing (runs YouTube and Vimeo analytics)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no database changes will be made)",
    )

    args = parser.parse_args()

    try:
        publisher = OnlineStatsPublisher()

        if args.process:
            # Run full analytics processing
            publisher.run_complete_process(args.csv, dry_run=args.dry_run)
        else:
            # Just publish from existing CSV
            publisher.publish_from_csv(args.csv, dry_run=args.dry_run)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
