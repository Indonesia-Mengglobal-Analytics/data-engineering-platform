"""Reading spreadsheets: from Google (service account) or from local fixture files."""

from __future__ import annotations

import json
from pathlib import Path

from contract import SheetSnapshot

SHEET_MIME = "application/vnd.google-apps.spreadsheet"
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


class SourceError(Exception):
    """A sheet that is listed but can't be read (no access, deleted)."""


def from_fixtures(folder: Path) -> list[SheetSnapshot | SourceError]:
    sheets: list[SheetSnapshot | SourceError] = []
    for path in sorted(folder.glob("*.json")):
        raw = json.loads(path.read_text())
        sheets.append(SheetSnapshot(raw["spreadsheet_id"], raw["title"], raw.get("modified_time"), raw["tabs"],
                                    frozenset(raw.get("unread_columns", []))))
    return sheets


class GoogleSource:
    def __init__(self, service_account_info: dict):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        self.sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self.account = service_account_info.get("client_email", "")

    def discover(self, folder_ids: list[str], spreadsheet_ids: list[str]) -> list[str]:
        """Every sheet in the folders (not recursive), then the listed IDs, without duplicates."""
        found: list[str] = []
        for folder_id in folder_ids:
            token = None
            while True:
                page = self.drive.files().list(
                    q=f"'{folder_id}' in parents and mimeType = '{SHEET_MIME}' and trashed = false",
                    fields="nextPageToken, files(id)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageToken=token,
                ).execute()
                found += [f["id"] for f in page.get("files", [])]
                token = page.get("nextPageToken")
                if not token:
                    break
        return list(dict.fromkeys(found + list(spreadsheet_ids)))

    def read(self, spreadsheet_id: str) -> SheetSnapshot | SourceError:
        from googleapiclient.errors import HttpError

        try:
            meta = self.drive.files().get(
                fileId=spreadsheet_id, fields="name, modifiedTime", supportsAllDrives=True
            ).execute()
            book = self.sheets.spreadsheets().get(
                spreadsheetId=spreadsheet_id, fields="sheets.properties.title"
            ).execute()
            titles = [s["properties"]["title"] for s in book.get("sheets", [])]
            values = self.sheets.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id, ranges=[f"'{t}'" for t in titles], valueRenderOption="FORMATTED_VALUE"
            ).execute()
        except HttpError as error:
            status = error.resp.status
            reason = (
                f"no access: share the sheet with {self.account}" if status in (403, 404) else f"HTTP {status}"
            )
            return SourceError(f"{spreadsheet_id}: {reason}")
        tabs = {t: r.get("values", []) for t, r in zip(titles, values.get("valueRanges", []))}
        return SheetSnapshot(spreadsheet_id, meta["name"], meta.get("modifiedTime"), tabs)
