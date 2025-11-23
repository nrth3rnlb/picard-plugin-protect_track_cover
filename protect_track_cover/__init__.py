# python
# -*- coding: utf-8 -*-

"""
Protect Track Cover Plugin for MusicBrainz Picard.
Warns if the tracks on an album use different covers
and there is a potential risk of these being replaced by the album cover.
"""

from __future__ import annotations

import os

from typing import Any

from PyQt5 import uic

PLUGIN_NAME = "Protect Track Cover"
PLUGIN_AUTHOR = "nrth3rnlb"
PLUGIN_DESCRIPTION = """
Protect Track Cover Plugin for MusicBrainz Picard.

Warns when tracks on an album use different covers and there is a
potential danger that these will be replaced by the album cover.
"""
PLUGIN_VERSION = "0.2.0"
PLUGIN_API_VERSIONS = ["2.0"]
PLUGIN_LICENSE = "GPL-2.0"
PLUGIN_LICENSE_URL = "https://www.gnu.org/licenses/gpl-2.0.html"

MUSICBRAINZ_ALBUMID = "musicbrainz_albumid"

import hashlib
import base64
import traceback as _traceback

from typing import Dict
from PyQt5.QtWidgets import QApplication


from typing import Optional

from mutagen import File
from mutagen.id3 import ID3
from mutagen.flac import FLAC, Picture
from mutagen.mp4 import MP4
from mutagen.mp3 import MP3

from picard import log

from picard.file import register_file_post_addition_to_track_processor
from picard.album import register_album_post_removal_processor

file_list: Dict[str, Dict[str, Any]] = {}  # album_id -> {'files': [paths], 'name': album_name}
# global aggregated state: value contains mapping + album name
all_album_mappings: Dict[str, Dict[str, Any]] = {}  # album_id -> {'mapping': {cover_hash: [paths]}, 'name': album_name}

all_albums_dialog: Optional[AlbumsWarningDialog] = None
# global aggregated state
all_dialog_closed: bool = False

from typing import Set
album_dialog_closed: Set[str] = set()

# new global to remember a short fingerprint of the mapping that was dismissed
album_dismissed_signature: Dict[str, str] = {}


def get_first_picture_bytes(path):
    """
    Return raw image bytes of the first embedded picture found in the file,
    or None if no embedded picture is present.
    Supports: MP3(ID3/APIC), FLAC (PICTURE), MP4 (covr), Ogg Vorbis/Opus (METADATA_BLOCK_PICTURE).
    """
    if not os.path.isfile(path):
        return None
    try:
        audio = File(path, easy=False)
        if audio is None:
            return None

        # MP3 / ID3
        try:
            if isinstance(audio, MP3):
                id3 = ID3(path)
                apics = id3.getall("APIC")
                if apics:
                    return apics[0].data
        except Exception:
            pass

        # FLAC
        try:
            if isinstance(audio, FLAC):
                pics = audio.pictures
                if pics:
                    return pics[0].data
        except Exception:
            pass

        # MP4 / M4A
        try:
            if isinstance(audio, MP4):
                covr = audio.tags.get("covr")
                if covr:
                    first = covr[0]
                    try:
                        return bytes(first)
                    except Exception:
                        return getattr(first, "data", None)
        except Exception:
            pass

        # Ogg Vorbis / Opus: metadata_block_picture (base64)
        try:
            tags = audio.tags
            if tags is not None:
                mbp = None
                for key in ("METADATA_BLOCK_PICTURE", "metadata_block_picture"):
                    if key in tags:
                        mbp = tags.get(key)
                        break
                if mbp:
                    b64 = mbp[0]
                    if isinstance(b64, bytes):
                        b64 = b64.decode("utf-8")
                    pic = Picture(base64.b64decode(b64))
                    return pic.data
        except Exception:
            pass

    except Exception:
        pass

    return None


def scan_files_for_embedded_covers(file_paths):
    """
    Scans a list of file paths and returns a dict:
      { cover_hash (str) | None_for_no_cover : [file_path, ...], ... }
    cover_hash is sha1(hex)
    """
    mapping = {}
    for path in file_paths:
        try:
            data = get_first_picture_bytes(path)
            if data:
                h = hashlib.sha1(data).hexdigest()
            else:
                h = None
            mapping.setdefault(h, []).append(path)
        except Exception:
            mapping.setdefault("error", []).append(path)
    return mapping


def format_scan_report(mapping):
    """
    Returns a human-readable report string from the mapping.
    """
    parts = []
    total_files = sum(len(v) for v in mapping.values())
    parts.append(f"Scanned {total_files} files.")
    for h, paths in mapping.items():
        if h is None:
            parts.append(f"- {len(paths)} files with no embedded cover:")
        elif h == "error":
            parts.append(f"- {len(paths)} files with read errors:")
        else:
            parts.append(f"- {len(paths)} files with cover hash {h}:")
        for p in paths[:10]:
            parts.append(f"    {os.path.basename(p)}")
        if len(paths) > 10:
            parts.append(f"    ... ({len(paths)-10} more)")
    return "\n".join(parts)



from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QDialog, QStyle

class AlbumsWarningDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        ui_file = os.path.join(os.path.dirname(__file__), 'albums_warning.ui')
        uic.loadUi(ui_file, self)

        self.setModal(False)
        self.setAttribute(Qt.WA_DeleteOnClose, True)
        self.closeButton.clicked.connect(self._on_close_clicked)

        # try theme icon first, then fallback to platform standard icon
        icon = QIcon.fromTheme("dialog-warning")
        if icon.isNull():
            icon = self.style().standardIcon(QStyle.SP_MessageBoxWarning)

        # choose size (use label fixed size from UI)
        size = self.iconLabel.maximumSize()
        if size.width() <= 0 or size.height() <= 0:
            pixmap = icon.pixmap(48, 48)
        else:
            pixmap = icon.pixmap(size.width(), size.height())

        self.iconLabel.setPixmap(pixmap)
        self.iconLabel.setScaledContents(False)

    def update_html(self, html: str = "", html_no_cover: str = "", html_errors: str = ""):
        self.browser.setHtml(html)
        self.browser_no_cover.setHtml(html_no_cover)
        self.browser_errors.setHtml(html_errors)
        try:
            self.show()
            self.raise_()
            self.activateWindow()
        except Exception:
            pass

    def _on_close_clicked(self):
        self.close()

def _format_album_block(album_id: str, mapping: Dict, album_name: Optional[str] = None) -> tuple[str, str, str]:
    """
    Create three HTML blocks for a single album showing grouped tracks.
    Returns tuple of (main_html, no_cover_html, errors_html)
    """
    parts = []
    parts_no_cover = []
    parts_errors = []

    album_label = album_name or album_id
    parts.append(f"<h3>{album_label}</h3>")
    # Filter and sort groups: display 'no cover' and 'error' last/first
    normal_groups = [(h, paths) for h, paths in mapping.items() if h not in (None, "error")]
    no_cover = mapping.get(None, [])
    errors = mapping.get("error", [])

    if not normal_groups and not no_cover and not errors:
        parts.append("<p><em>No files scanned for this album yet.</em></p>")
        return "\n".join(parts)

    # Label cover groups without revealing checksums
    for idx, (_h, paths) in enumerate(sorted(normal_groups, key=lambda x: -len(x[1])), start=1):
        parts.append(f"<b>Cover group {idx} — {len(paths)} file(s):</b>")
        parts.append("<ul>")
        for p in paths:
            parts.append(f"<li>{os.path.basename(p)}</li>")
        parts.append("</ul>")

    if no_cover:
        parts_no_cover.append(f"<b>No embedded cover — {len(no_cover)} file(s):</b>")
        parts_no_cover.append("<ul>")
        for p in no_cover:
            parts_no_cover.append(f"<li>{os.path.basename(p)}</li>")
        parts_no_cover.append("</ul>")

    if errors:
        parts_errors.append(f"<b>Read errors — {len(errors)} file(s):</b>")
        parts_errors.append("<ul>")
        for p in errors:
            parts_errors.append(f"<li>{os.path.basename(p)}</li>")
        parts_errors.append("</ul>")

    return "\n".join(parts), "\n".join(parts_no_cover), "\n".join(parts_errors)

def format_aggregated_report(all_mappings: Dict[str, Dict]) -> tuple[str, str, str]:
    """Build full HTML report for all albums that currently have differing covers.
    Accepts values in the shape {'mapping': {...}, 'name': '...'} for each album_id.
    """
    parts: list[str] = ["<html><body>"]
    parts_no_cover: list[str] = ["<html><body>"]
    parts_errors: list[str] = ["<html><body>"]
    any_shown = False
    for album_id, entry in all_mappings.items():
        # support both old and new shapes (entry may be mapping directly)
        mapping = entry.get("mapping") if isinstance(entry, dict) and "mapping" in entry else entry
        album_name = entry.get("name") if isinstance(entry, dict) else None

        real_hashes = [k for k in mapping.keys() if k not in (None, "error")]
        distinct = set(real_hashes)
        different_covers = len(distinct) > 1 or (len(distinct) == 1 and None in mapping)
        if different_covers:
            any_shown = True
            part, no_cover, errors = _format_album_block(album_id, mapping, album_name)
            if part:
                parts.append(part)
            if no_cover:
                parts_no_cover.append(no_cover)
            if errors:
                parts_errors.append(errors)

    if not any_shown:
        parts.append("<p><em>No albums with differing embedded covers at the moment.</em></p>")
    parts.append("</body></html>")

    if not parts_no_cover or len(parts_no_cover) == 1:
        parts_no_cover.append("<p><em>No albums with files lacking embedded covers at the moment.</em></p>")
    parts_no_cover.append("</body></html>")

    if not parts_errors or len(parts_errors) == 1:
        parts_errors.append("<p><em>No albums with file read errors at the moment.</em></p>")
    parts_errors.append("</body></html>")

    return "\n".join(parts), "\n".join(parts_no_cover), "\n".join(parts_errors)

def warn_if_multiple_covers(mapping, parent_widget=None, album_id: Optional[str] = None, album_name: Optional[str] = None):
    """
    Non-blocking aggregated warning for all albums.
    - Updates internal per-album mapping and stored album name.
    - Maintains a single dialog for all albums
    """
    global all_albums_dialog, all_album_mappings, album_dialog_closed

    # compute whether this album has multiple covers
    real_hashes = [k for k in mapping.keys() if k not in (None, "error")]
    distinct = set(real_hashes)
    different_covers = len(distinct) > 1 or (len(distinct) == 1 and None in mapping)

    # update aggregated state for this album_id (store mapping + name)
    if album_id:
        if not different_covers:
            all_album_mappings.pop(album_id, None)
        else:
            all_album_mappings[album_id] = {'mapping': {k: list(v) for k, v in mapping.items()},
                                           'name': album_name}

    # Build filtered report only for albums that have differing covers and are not dismissed
    albums_to_show = get_albums_to_show(album_dialog_closed, all_album_mappings)

    # If nothing remaining to show, close existing dialog if present and return
    if not albums_to_show:
        try:
            if all_albums_dialog is not None:
                try:
                    all_albums_dialog.close()
                except Exception:
                    pass
                globals().update({'all_albums_dialog': None})
        except Exception:
            pass
        return True

    # Build aggregated HTML only for albums_to_show
    filtered = {aid: all_album_mappings[aid] for aid in albums_to_show}
    report_html, report_no_cover_html, report_errors_html = format_aggregated_report(filtered)

    app = QApplication.instance()
    created_app = False
    if app is None:
        app = QApplication([])
        created_app = True

    try:
        if all_albums_dialog is None:
            all_albums_dialog = AlbumsWarningDialog(parent_widget)
            # ensure deletion from variable when window is destroyed
            try:
                all_albums_dialog.destroyed.connect(lambda _: globals().update({'all_albums_dialog': None}))
            except Exception:
                pass
        all_albums_dialog.update_html(report_html, report_no_cover_html, report_errors_html)
    except Exception as e:
        log.error("%s: Failed updating/creating aggregated dialog: %s", PLUGIN_NAME, e)
        log.error("%s: Traceback: %s", PLUGIN_NAME, _traceback.format_exc())

    if created_app:
        # do not quit the app because Picard manages the main loop;
        # app was created only to allow widget creation in tests/environments.
        pass

    return True

def get_albums_to_show(dialog_closed: set[str], album_mappings: dict[str, dict[str, Any]]) -> list[str]:
    albums_to_show = []
    for aid, entry in album_mappings.items():
        m = entry.get('mapping') if isinstance(entry, dict) and 'mapping' in entry else entry
        real_hashes = [k for k in m.keys() if k not in (None, "error")]
        distinct = set(real_hashes)
        diff = len(distinct) > 1 or (len(distinct) == 1 and None in m)
        if diff and aid not in dialog_closed:
            albums_to_show.append(aid)
    return albums_to_show

def protect_track_cover(track, file: Any):
    try:
        log.debug("%s: Processing file: %s", PLUGIN_NAME, getattr(file, "filename", "<unknown>"))

        if not file.metadata.get(MUSICBRAINZ_ALBUMID):
            log.debug("%s: Skipping for track in %s as %s is required.", PLUGIN_NAME, track.album, MUSICBRAINZ_ALBUMID)
            return

        album_id = file.metadata.get(MUSICBRAINZ_ALBUMID)
        # normalise if Picard gives a list/tuple
        if isinstance(album_id, (list, tuple)):
            album_id = album_id[0] if album_id else None

        if not album_id:
            return

        album = track.album.metadata.get("album") or "<unknown>"

        # track files per album and avoid duplicates; store album name too
        file_list.setdefault(album_id, {"files": [], "name": album})
        if album and not file_list[album_id].get("name"):
            file_list[album_id]["name"] = album

        if getattr(file, "filename", None) and file.filename not in file_list[album_id]["files"]:
            file_list[album_id]["files"].append(file.filename)

        # scan embedded covers for the tracked files of this album
        mapping = scan_files_for_embedded_covers(file_list[album_id]["files"])

        for h, paths in mapping.items():
            log.debug("%s: cover=%s files=%s", PLUGIN_NAME, h, ", ".join(os.path.basename(p) for p in paths))

        # non-blocking warning/update dialog per album (pass album_name)
        try:
            warn_if_multiple_covers(mapping, album_id=album_id, album_name=file_list[album_id].get("name"))
        except Exception:
            log.error("%s: Error showing/updating warning dialog for album %s: %s", PLUGIN_NAME, album_id, _traceback.format_exc())

    except Exception as e:
        log.error("%s: Error in protect_track_cover: %s", PLUGIN_NAME, e)
        log.error("%s: Traceback: %s", PLUGIN_NAME, _traceback.format_exc())


def on_album_removed(album: Any):
    """
    Cleanup internal state for an album when Picard removes it.
    Called via register_album_post_removal_processor(...).
    """
    try:
        # determine album id robustly
        album_id = None
        try:
            album_metadata = getattr(album, "metadata", None)
            if isinstance(album_metadata, dict):
                album_id = album_metadata.get(MUSICBRAINZ_ALBUMID)
        except Exception:
            pass
        if not album_id:
            album_id = getattr(album, "albumid", None) or getattr(album, "id", None)
        if isinstance(album_id, (list, tuple)):
            album_id = album_id[0] if album_id else None
        if not album_id:
            return

        # remove tracked state for this album
        all_album_mappings.pop(album_id, None)
        file_list.pop(album_id, None)
        album_dialog_closed.discard(album_id)
        try:
            album_dismissed_signature.pop(album_id, None)
        except Exception:
            pass

        # Update or close the aggregated dialog depending on remaining albums to show
        try:
            # compute remaining albums that still need warnings and are not dismissed
            albums_to_show = get_albums_to_show(album_dialog_closed, all_album_mappings)
            if not albums_to_show:
                if all_albums_dialog is not None:
                    try:
                        all_albums_dialog.close()
                    except Exception:
                        pass
                    globals().update({'all_albums_dialog': None})
            else:
                # refresh dialog contents for remaining albums
                filtered = {aid: all_album_mappings[aid] for aid in albums_to_show}
                if all_albums_dialog is not None:
                    try:
                        report_html, report_no_cover_html, report_errors_html = format_aggregated_report(filtered)
                        all_albums_dialog.update_html(report_html, report_no_cover_html, report_errors_html)
                    except Exception:
                        pass
        except Exception:
            log.error("%s: Error updating dialog after album removal: %s", PLUGIN_NAME, _traceback.format_exc())

    except Exception:
        log.error("%s: Error in on_album_removed: %s", PLUGIN_NAME, _traceback.format_exc())

log.debug(PLUGIN_NAME + ": registration")
register_file_post_addition_to_track_processor(protect_track_cover)
register_album_post_removal_processor(on_album_removed)
