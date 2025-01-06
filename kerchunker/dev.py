import asyncio
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Container is now active and ready for commands...")

    try:
        # Keep the container running indefinitely
        while True:
            await asyncio.sleep(60)  # Sleep for 60 seconds between checks
            logger.info("Container is still active...")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
