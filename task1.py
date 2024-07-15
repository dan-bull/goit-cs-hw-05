import asyncio
import aiofiles
import argparse
import logging
from pathlib import Path

def parse_arguments():
    parser = argparse.ArgumentParser(description="Async file sorter based on file extensions.")
    parser.add_argument('source_folder', type=str, help='Source folder path')
    parser.add_argument('output_folder', type=str, help='Output folder path')
    return parser.parse_args()

args = parse_arguments()

source_folder = Path(args.source_folder)
output_folder = Path(args.output_folder)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def read_folder(folder: Path):
    files = []
    try:
        for entry in folder.iterdir():
            if entry.is_dir():
                files.extend(await read_folder(entry))
            else:
                files.append(entry)
    except Exception as e:
        logger.error(f"Error reading folder {folder}: {e}")
    return files

async def copy_file(file: Path, output_folder: Path):
    ext = file.suffix.lstrip('.')
    if ext:
        dest_folder = output_folder / ext
    else:
        dest_folder = output_folder / 'no_extension'
    
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest_path = dest_folder / file.name
    
    try:
        async with aiofiles.open(file, 'rb') as fsrc:
            async with aiofiles.open(dest_path, 'wb') as fdst:
                await fdst.write(await fsrc.read())
        logger.info(f'Copied {file} to {dest_path}')
    except Exception as e:
        logger.error(f"Error copying file {file} to {dest_path}: {e}")

async def main():
    if not source_folder.exists():
        logger.error(f"Source folder {source_folder} does not exist.")
        return

    if not source_folder.is_dir():
        logger.error(f"Source folder {source_folder} is not a directory.")
        return
    
    try:
        files = await read_folder(source_folder)
        tasks = [copy_file(file, output_folder) for file in files]
        await asyncio.gather(*tasks)
        logger.info("All files have been processed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == '__main__':
    args = parse_arguments()  # Ensure arguments are parsed before running main
    source_folder = Path(args.source_folder)
    output_folder = Path(args.output_folder)
    asyncio.run(main())
