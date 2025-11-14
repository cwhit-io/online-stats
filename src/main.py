#!/usr/bin/env python3
"""
Main script to run Vimeo and YouTube analytics and publish results to database.
"""

import os
import sys
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
            import psycopg2

            return psycopg2.connect(**self.db_config)
        except Exception as e:
            print(f"Database connection error: {e}")
            raise

    def run_analytics(self, start_date=None, end_date=None):
        """Run both Vimeo and YouTube analytics."""
        print("Starting Online Video Statistics Processing")
        print("=" * 50)

        # Parse date arguments
        if start_date:
            try:
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(
                    f"Invalid start date format: {start_date}. Use YYYY-MM-DD format."
                )

        if end_date:
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(
                    f"Invalid end date format: {end_date}. Use YYYY-MM-DD format."
                )

        if not start_date or not end_date:
            raise ValueError("Both start_date and end_date must be provided")

        try:
            # Step 1: Run YouTube analytics
            print("\n1. Running YouTube Analytics...")
            youtube_finder = YouTubeLiveViewsFinder()
            youtube_results = youtube_finder.process_date_range(start_date, end_date)

            # Step 2: Run Vimeo analytics
            print("\n2. Running Vimeo Analytics...")
            vimeo_finder = VimeoLiveViewsFinder()
            vimeo_results = vimeo_finder.process_date_range(start_date, end_date)

            # Step 3: Merge results
            print("\n3. Merging YouTube and Vimeo results...")
            merged_results = self.merge_analytics_results(
                youtube_results, vimeo_results
            )

            print("\n✅ Analytics processing completed successfully!")

            return merged_results

        except Exception as e:
            print(f"❌ Error during analytics processing: {e}")
            raise

    def merge_analytics_results(self, youtube_results, vimeo_results):
        """Merge YouTube and Vimeo results by date."""
        if not youtube_results and not vimeo_results:
            return []

        # Create dictionaries keyed by date for easy merging
        youtube_dict = {row["date"]: row for row in youtube_results or []}
        vimeo_dict = {row["date"]: row for row in vimeo_results or []}

        # Get all unique dates
        all_dates = set(youtube_dict.keys()) | set(vimeo_dict.keys())

        merged_results = []
        for date in sorted(all_dates):
            youtube_row = youtube_dict.get(date, {})
            vimeo_row = vimeo_dict.get(date, {})

            merged_row = {
                "date": date,
                "youtube_9am": youtube_row.get("youtube_9am"),
                "youtube_1045am": youtube_row.get("youtube_1045am"),
                "youtube_notes": youtube_row.get("youtube_notes", ""),
                "vimeo_9am": vimeo_row.get("vimeo_9am"),
                "vimeo_1045am": vimeo_row.get("vimeo_1045am"),
                "vimeo_notes": vimeo_row.get("vimeo_notes", ""),
            }
            merged_results.append(merged_row)

        return merged_results

    def publish_to_database(
        self, stats, data_date=None, dry_run=False, overwrite=False
    ):
        """Publish statistics to the database."""
        if dry_run:
            print("🔍 DRY RUN: Would publish the following statistics to database:")
            print(f"   Date: {data_date}")
            print(f"   YouTube 9AM: {stats['youtube_9am']}")
            print(f"   Vimeo 10:45AM: {stats['vimeo_1045am']}")
            print(f"   Vimeo 9AM: {stats['vimeo_9am']}")
            print(f"   YouTube 10:45AM: {stats['youtube_1045am']}")
            print(f"   Overwrite: {overwrite}")
            print(f"   Created At: {datetime.now()}")
            print("✅ Dry run completed - no database changes made!")
            return

        conn = None
        cursor = None

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # Check if data already exists for this date
            if data_date:
                select_query = """
                SELECT youtube_9am, vimeo_1045am, vimeo_9am, youtube_1045am
                FROM online_stats
                WHERE date = %s
                """
                cursor.execute(select_query, (data_date,))
                existing_row = cursor.fetchone()

                if existing_row:
                    # Check if any columns have actual data (not null)
                    has_data = any(val is not None for val in existing_row)

                    if has_data and not overwrite:
                        print(
                            f"ℹ️  Data for date {data_date} already exists with values in database. Skipping insert."
                        )
                        print(f"   Use --overwrite to force update existing data.")
                        return
                    elif has_data and overwrite:
                        # Update existing record
                        update_query = """
                        UPDATE online_stats
                        SET youtube_9am = %s, vimeo_1045am = %s, vimeo_9am = %s, youtube_1045am = %s, updated_at = %s
                        WHERE date = %s
                        """
                        updated_at = datetime.now()
                        cursor.execute(
                            update_query,
                            (
                                stats["youtube_9am"],
                                stats["vimeo_1045am"],
                                stats["vimeo_9am"],
                                stats["youtube_1045am"],
                                updated_at,
                                data_date,
                            ),
                        )
                        conn.commit()
                        print(
                            "✅ Successfully updated existing statistics in database!"
                        )
                        print(f"   Date: {data_date}")
                        print(f"   YouTube 9AM: {stats['youtube_9am']}")
                        print(f"   Vimeo 10:45AM: {stats['vimeo_1045am']}")
                        print(f"   Vimeo 9AM: {stats['vimeo_9am']}")
                        print(f"   YouTube 10:45AM: {stats['youtube_1045am']}")
                        return
                    # If no data exists, fall through to INSERT

            # Insert new record
            insert_query = """
            INSERT INTO online_stats (date, youtube_9am, vimeo_1045am, vimeo_9am, youtube_1045am, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            created_at = datetime.now()

            cursor.execute(
                insert_query,
                (
                    data_date,
                    stats["youtube_9am"],
                    stats["vimeo_1045am"],
                    stats["vimeo_9am"],
                    stats["youtube_1045am"],
                    created_at,
                ),
            )

            conn.commit()

            print("✅ Successfully published statistics to database!")
            print(f"   Date: {data_date}")
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

    def run_complete_process(
        self,
        dry_run=False,
        overwrite=False,
        start_date=None,
        end_date=None,
    ):
        """Run the complete process: analytics + database publishing."""
        if dry_run:
            print("🔍 DRY RUN MODE: No actual changes will be made to the database")
            print("=" * 60)

        try:
            # Step 1: Run analytics and get merged results
            merged_results = self.run_analytics(
                start_date=start_date, end_date=end_date
            )

            # Step 2: Publish each result to database
            if merged_results:
                print(f"\n4. Publishing {len(merged_results)} results to database...")
                for result in merged_results:
                    # Extract stats for this date
                    stats = {
                        "youtube_9am": result.get("youtube_9am"),
                        "vimeo_1045am": result.get("vimeo_1045am"),
                        "vimeo_9am": result.get("vimeo_9am"),
                        "youtube_1045am": result.get("youtube_1045am"),
                    }

                    # Publish to database (or simulate in dry-run mode)
                    self.publish_to_database(
                        stats, result["date"], dry_run=dry_run, overwrite=overwrite
                    )
            else:
                print("\n⚠️ No results to publish.")

            if dry_run:
                print("\n🎭 Dry run completed successfully - no database changes made!")
            else:
                print("\n🎉 Complete process finished successfully!")

        except Exception as e:
            print(f"\n💥 Process failed: {e}")
            sys.exit(1)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Process online video statistics and publish to database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no database changes will be made)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data even if columns already have values",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date for processing (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date for processing (YYYY-MM-DD format)",
    )

    args = parser.parse_args()

    try:
        publisher = OnlineStatsPublisher()

        # Run analytics processing and publish to database
        publisher.run_complete_process(
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
