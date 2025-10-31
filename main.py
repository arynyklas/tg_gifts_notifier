"""
Main entry point for TG Gifts Notifier.
"""
import sys
import asyncio

from core.detector import main as detector_main

try:
    import config
except ImportError:
    print("Error: config.py not found!")
    print("Please copy config.example.py to config.py and configure it.")
    sys.exit(1)


if __name__ == "__main__":
    # Check for save-only flag
    save_only = "--save-only" in sys.argv or "-S" in sys.argv
    
    try:
        asyncio.run(detector_main(save_only=save_only))
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

