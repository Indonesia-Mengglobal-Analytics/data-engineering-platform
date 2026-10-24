"""Where the lakehouse and its spreadsheet outputs live.

DriveStorage keeps files in a Google Shared Drive folder (the service account is a Content manager
there; a service account has no My Drive storage of its own). LocalStorage keeps the same layout in a
directory, for tests and for runs without a Shared Drive (history then does not survive between runs).
Paths are "/"-separated and relative to the root, e.g. "lakehouse/silver/mentee.parquet".
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

FOLDER_MIME = "application/vnd.google-apps.folder"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"


class LocalStorage:
    kind = "local"

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def read(self, path: str) -> bytes | None:
        p = self.root / path
        return p.read_bytes() if p.exists() else None

    def write(self, path: str, data: bytes) -> None:
        p = self.root / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def link(self, path: str) -> str | None:
        return None


class DriveStorage:
    kind = "drive"

    def __init__(self, drive, root_folder_id: str):
        self.drive = drive
        self.root = root_folder_id
        self._folders: dict[str, str] = {"": root_folder_id}

    def _list(self, q: str, fields: str = "files(id, name, createdTime, mimeType)") -> list[dict]:
        out, token = [], None
        while True:
            page = self.drive.files().list(q=q, fields=f"nextPageToken, {fields}", supportsAllDrives=True,
                                           includeItemsFromAllDrives=True, pageToken=token).execute()
            out += page.get("files", [])
            token = page.get("nextPageToken")
            if not token:
                return out

    def folder(self, path: str) -> str:
        """ID of the folder at path, created if missing."""
        path = path.strip("/")
        if path in self._folders:
            return self._folders[path]
        parent_path, _, name = path.rpartition("/")
        parent = self.folder(parent_path)
        found = self._list(f"'{parent}' in parents and name = '{name}' and mimeType = '{FOLDER_MIME}' and trashed = false")
        if found:
            fid = found[0]["id"]
        else:
            fid = self.drive.files().create(body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent]},
                                            fields="id", supportsAllDrives=True).execute()["id"]
        self._folders[path] = fid
        return fid

    def _file(self, path: str) -> dict | None:
        folder_path, _, name = path.strip("/").rpartition("/")
        parent = self.folder(folder_path)
        found = self._list(f"'{parent}' in parents and name = '{name}' and trashed = false")
        return found[0] if found else None

    def read(self, path: str) -> bytes | None:
        from googleapiclient.http import MediaIoBaseDownload

        meta = self._file(path)
        if not meta:
            return None
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, self.drive.files().get_media(fileId=meta["id"], supportsAllDrives=True))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def write(self, path: str, data: bytes) -> None:
        from googleapiclient.http import MediaIoBaseUpload

        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/octet-stream", resumable=False)
        meta = self._file(path)
        if meta:
            self.drive.files().update(fileId=meta["id"], media_body=media, supportsAllDrives=True).execute()
        else:
            folder_path, _, name = path.strip("/").rpartition("/")
            self.drive.files().create(body={"name": name, "parents": [self.folder(folder_path)]}, media_body=media,
                                      fields="id", supportsAllDrives=True).execute()

    def link(self, path: str) -> str | None:
        return f"https://drive.google.com/drive/folders/{self.folder(path)}"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
