#!/usr/bin/env python3
"""
FastAPI server for Online Video Statistics Analyzer
"""

import subprocess
import sys
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn

app = FastAPI(
    title="Online Video Statistics API",
    description="API for analyzing video statistics from YouTube and Vimeo",
    version="1.0.0",
)


class AnalyticsRequest(BaseModel):
    start_date: str
    end_date: str
    dry_run: Optional[bool] = False
    overwrite: Optional[bool] = False


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Online Video Statistics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/analytics", response_model=TaskResponse)
async def run_analytics(request: AnalyticsRequest, background_tasks: BackgroundTasks):
    """
    Run video analytics for the specified date range.

    - **start_date**: Start date in YYYY-MM-DD format
    - **end_date**: End date in YYYY-MM-DD format
    - **dry_run**: If true, run in dry-run mode (no database changes)
    - **overwrite**: If true, overwrite existing data
    """
    try:
        # Validate date format
        from datetime import datetime

        datetime.strptime(request.start_date, "%Y-%m-%d")
        datetime.strptime(request.end_date, "%Y-%m-%d")

        # Build command arguments
        cmd = [
            sys.executable,
            "src/main.py",
            "--start-date",
            request.start_date,
            "--end-date",
            request.end_date,
        ]

        if request.dry_run:
            cmd.append("--dry-run")

        if request.overwrite:
            cmd.append("--overwrite")

        # Run the analytics in the background
        background_tasks.add_task(run_analytics_task, cmd)

        return TaskResponse(
            task_id=f"analytics_{request.start_date}_{request.end_date}",
            status="started",
            message=f"Analytics started for date range {request.start_date} to {request.end_date}",
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting analytics: {e}")


async def run_analytics_task(cmd):
    """Run the analytics command in the background."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/app" if "/app" in sys.path[0] else None,
        )

        if result.returncode == 0:
            print(f"✅ Analytics completed successfully: {cmd}")
        else:
            print(f"❌ Analytics failed with return code {result.returncode}: {cmd}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

    except Exception as e:
        print(f"❌ Error running analytics task: {e}")


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
