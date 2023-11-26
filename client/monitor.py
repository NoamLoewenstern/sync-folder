import argparse
import sys
import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from collections import deque
import requests
from utils import debounce

logger = logging.getLogger(__name__)


def EventHandlerWithDebounce(debounce_ms: int):
    class UploadFileEventHandler(FileSystemEventHandler):
        def __init__(self, *, upload_url: str, logger: logging.Logger):
            self.logger = logger
            self.upload_url = upload_url
            self.upload_queue: deque[str] = deque()

        def on_moved(self, event):  # type: ignore
            if event.is_directory:  # type: ignore
                print("cannot move directory")
                return
            print(f"{event.src_path} has been moved to {event.dest_path}")  # type: ignore
            self.upload_files(event.dest_path)  # type: ignore

        def on_created(self, event):  # type: ignore
            if event.is_directory:  # type: ignore
                print("cannot move directory")
                return
            print(f"{event.src_path} has been created")  # type: ignore
            self.upload_files(event.src_path)  # type: ignore

        def on_deleted(self, event):  # type: ignore
            if event.is_directory:  # type: ignore
                print("cannot move directory")
                return
            print(f"{event.src_path} has been deleted")  # type: ignore
            self.upload_files(event.src_path)  # type: ignore

        def on_modified(self, event):  # type: ignore
            if event.is_directory:  # type: ignore
                print("cannot move directory")
                return
            print(f"{event.src_path} has been modified")  # type: ignore
            self.upload_files(event.src_path)  # type: ignore

        @debounce(debounce_ms)
        def debounced_upload_from_queue(self):
            filepaths = list(self.upload_queue)
            print(f"Uploading {len(filepaths)} files")
            self.upload_queue.clear()

            files = []
            try:
                files = [("files", open(filepath, "rb")) for filepath in filepaths]
                resp = requests.post(self.upload_url, files=files, verify=False)
                resp.raise_for_status()
            except Exception as e:
                self.logger.exception(f"Failed to upload: {filepaths} {str(e)}")
            finally:
                # close file handlers
                for file in files:
                    file[1].close()

        def upload_files(self, filepaths: list[str] | str):
            filepaths = [filepaths] if isinstance(filepaths, str) else filepaths
            for path in filepaths:
                if not Path(path).exists():
                    self.logger.error(f"file {path!r} does not exist")
                    continue
                if Path(path).is_dir():
                    self.logger.error(f"cannot upload directory {path!r}")
                    continue
                # already in queue
                if path in self.upload_queue:
                    continue
                self.upload_queue.append(str(path))
            self.debounced_upload_from_queue()

    return UploadFileEventHandler


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--directory", "-d", required=True, type=Path, help="path to file to upload"
    )
    parser.add_argument(
        "--url",
        "-u",
        required=True,
        type=str,
        help="url to upload file to",
    )
    parser.add_argument(
        "--debounce",
        "-b",
        required=False,
        type=int,
        default=3000,
        help="ms to wait before uploading",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not args.directory.exists():
        logging.error(f"directory {args.directory!r} does not exist")
        sys.exit(1)
    logging.info(f"start watching directory {args.directory!r}")
    event_handler = EventHandlerWithDebounce(args.debounce)(
        upload_url=args.url, logger=logger
    )
    observer = Observer()
    observer.schedule(event_handler, args.directory, recursive=True)  # type: ignore
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
