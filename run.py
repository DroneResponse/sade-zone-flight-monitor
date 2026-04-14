#!/usr/bin/env python3
"""Thin entry point for the SADE telemetry pipeline.

Usage:
    python run.py [options]

All options are passed through to app.main. Run with --help for details.
"""
from app.main import main

if __name__ == "__main__":
    raise SystemExit(main())
