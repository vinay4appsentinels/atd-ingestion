#!/usr/bin/env python3
"""
Wrapper script to run ATD Ingestion Service with proper signal handling
"""

import signal
import sys
import os
import time
from atd_ingestion.service import ATDIngestionService

# Global flag to track if we're already shutting down
shutting_down = False

def main():
    """Run the service with proper shutdown handling"""
    global shutting_down
    service = None
    
    def signal_handler(signum, frame):
        """Handle shutdown signals"""
        global shutting_down
        if shutting_down:
            print("\nForce killing due to repeated interrupt...")
            os._exit(1)
        
        shutting_down = True
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        print("Press Ctrl+C again to force quit")
        
        if service:
            try:
                service.stop()
                # Give it a moment to stop
                time.sleep(0.5)
                service.shutdown()
            except Exception as e:
                print(f"Error during shutdown: {e}")
        
        os._exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Parse command line arguments
        import argparse
        import os
        parser = argparse.ArgumentParser(description="ATD Ingestion Service")
        parser.add_argument(
            '--config',
            type=str,
            default='config/config.yaml',
            help='Path to configuration file'
        )
        parser.add_argument(
            '--topic',
            type=str,
            help='Kafka topic to consume from (overrides config file)'
        )
        
        args = parser.parse_args()
        
        # Check for topic override from environment variable
        topic_override = args.topic or os.getenv('KAFKA_TOPIC')
        
        # Create and start service
        service = ATDIngestionService(args.config, topic_override)
        
        # Remove the duplicate signal handlers from service
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        service.start()
        
    except KeyboardInterrupt:
        # This shouldn't happen due to signal handler, but just in case
        pass
    except Exception as e:
        print(f"Service failed: {str(e)}")
        if service:
            try:
                service.shutdown()
            except:
                pass
        sys.exit(1)

if __name__ == "__main__":
    main()