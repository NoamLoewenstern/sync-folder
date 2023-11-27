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
from contextlib import ExitStack

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60
BATCH_MAX_SIZE_IN_BYTES = 30 * 1024 * 1024  # 30MB


def filter_out_max_size(
    filepaths: list[str], max_size_in_bytes: int = BATCH_MAX_SIZE_IN_BYTES
) -> list[str]:
    """Returns a list of filepaths that are less than max_size_in_bytes"""
    files: list[str] = []
    for filepath in filepaths:
        if Path(filepath).stat().st_size > max_size_in_bytes:
            logger.error(f"file {filepath!r} is too large to upload")
            continue
        files.append(filepath)
    return files


def group_by_chunks_of_max_size(
    filepaths: list[str], max_sum_size: int = BATCH_MAX_SIZE_IN_BYTES
) -> list[list[str]]:
    """Returns a list of filepaths that are less than max_sum_size"""
    files: list[list[str]] = [[]]
    sum_size = 0
    for filepath in filepaths:
        filepath_size = Path(filepath).stat().st_size
        if sum_size + filepath_size > max_sum_size:
            files.append([filepath])
            sum_size = filepath_size
        else:
            files[-1].append(filepath)
            sum_size += filepath_size
    return files


def upload_files(filepaths: list[str], upload_url: str):
    with ExitStack() as stack:
        files = [
            ("files", stack.enter_context(open(filepath, "rb")))
            for filepath in filepaths
        ]
        try:
            resp = requests.post(
                upload_url,
                files=files,
                verify=False,
                timeout=DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            logger.info(f"Uploaded {len(filepaths)} files")
        except Exception as e:
            logger.exception(f"Failed to upload: {filepaths} {str(e)}")
            raise


def secure_upload_files(filepaths: list[str], upload_url: str):
    filepaths = filter_out_max_size(filepaths)
    chunks_of_filepaths = group_by_chunks_of_max_size(filepaths)
    for filepaths in chunks_of_filepaths:
        upload_files(filepaths, upload_url)


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
            filepaths = filter_out_max_size(filepaths)
            chunks_of_filepaths = group_by_chunks_of_max_size(filepaths)

            try:
                for filepaths in chunks_of_filepaths:
                    upload_files(filepaths, self.upload_url)
                for filepath in filepaths:
                    self.upload_queue.remove(filepath)
            except Exception as e:
                self.logger.exception(f"Failed to upload: {filepaths} {str(e)}")

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
                if Path(path).stat().st_size > BATCH_MAX_SIZE_IN_BYTES:
                    self.logger.error(f"file {path!r} is too large to upload")
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
    parser.add_argument(
        "--init-upload",
        action="store_true",
        help="upload all files on first start",
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

    if args.init_upload:
        secure_upload_files(list(args.directory.glob("*")), args.url)

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
