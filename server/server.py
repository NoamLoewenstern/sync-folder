from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Request, UploadFile
from pathlib import Path
import argparse
from fastapi.responses import RedirectResponse
import uvicorn
import logging


log_config = uvicorn.config.LOGGING_CONFIG  # type: ignore
log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
logger = logging.getLogger("uvicorn.access")


@asynccontextmanager
async def lifespan(app: FastAPI):
    args = get_args()
    directory = Path(args.directory)
    if not directory.is_dir():
        raise ValueError(f"{directory} is not a valid directory")

    app.state.directory = directory

    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def redirect_to_docs():
    return RedirectResponse(url="/docs")


def get_output_dir(request: Request) -> Path:
    return request.app.state.directory


@app.post("/upload")
async def upload_files(
    files: list[UploadFile], directory: Path = Depends(get_output_dir)
):
    resp = {
        "total": len(files),
        "success": 0,
        "failed": 0,
    }
    for file in files:
        if not file.filename:
            logger.warning("No filename")
            resp["failed"] += 1
            continue
        file_path = directory / file.filename
        print(f"Saving: {file_path}")
        contents = await file.read()
        file_path.write_bytes(contents)
        resp["success"] += 1
    return resp


def get_args():
    parser = argparse.ArgumentParser(description="FastAPI Server")
    parser.add_argument(
        "--directory",
        "-d",
        required=True,
        type=Path,
        help="Directory to save uploaded files",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        help="Port to run the server on",
        default=8000,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Port to run the server on",
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()

    print("Starting server...")
    uvicorn.run(  # type: ignore
        "server:app",
        host="0.0.0.0",
        port=args.port,
        reload=args.debug,
        log_config=log_config,  # type: ignore
    )


if __name__ == "__main__":
    main()
