# python
# -*- coding: utf-8 -*-

"""
Protect Track Cover Plugin for MusicBrainz Picard.
Warns if the tracks on an album use different covers
and there is a potential risk of these being replaced by the album cover.
"""

from __future__ import annotations

import base64
import os
from typing import Any, List

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
import traceback as _traceback

from typing import Dict
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QVBoxLayout, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QGroupBox
from PyQt5.QtGui import QPixmap

try:
    from PIL import Image, UnidentifiedImageError

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

from typing import Set

from mutagen import File
from mutagen.id3 import ID3
from mutagen.flac import FLAC, Picture
from mutagen.mp4 import MP4
from mutagen.mp3 import MP3

from picard import log

from picard.file import register_file_post_addition_to_track_processor
from picard.album import register_album_post_removal_processor

_NO_ALBUM_KEY = "no_album"
_HASH_ERROR = "error"

THUMBNAIL_MAX_SIZE = 100
IMAGE_MAX_DIMENSION = 4096
MAX_IMAGE_SIZE_BYTES = 10 * 1024 * 1024

_THUMBNAIL_UNAVAILABLE_HTML = ('<div style="color: gray; font-style: italic;">Image invalid or too large for '
                               'preview.</div>')
_THUMBNAIL_PIL_MISSING_HTML = '<div style="color: gray; font-style: italic;">Python Image Library is missing.</div>'
# Global cache: per-album file hash cache
# Structure: album_id -> { path -> {'hash':..., 'mtime':..., 'size':..., 'thumbnail_html':...} }
album_file_hash_cache: Dict[str, Dict[str, Any]] = {}

_pending_warn_albums: Set[str] = set()
# debounce time for warnings
_WARN_DEBOUNCE_MILLISECONDS = 500

file_list: Dict[str, Dict[str, Any]] = {}  # album_id -> {'files': [paths], 'name': album_name}
# global aggregated state: value contains mapping + album name
all_album_mappings: Dict[str, Dict[str, Any]] = {}  # album_id -> {'mapping': {cover_hash: [paths]}, 'name': album_name}

all_albums_dialog: AlbumsWarningDialog | None = None
# global aggregated state
all_dialog_closed: bool = False

from typing import Set
album_dialog_closed: Set[str] = set()

# new global to remember a short fingerprint of the mapping that was dismissed
album_dismissed_signature: Dict[str, str] = {}

def _get_file_stat(path: str) -> tuple[float | None, int | None]:
    try:
        st = os.stat(path)
        return st.st_mtime, st.st_size
    except IOError:
        return None, None

def _is_cache_valid(cached: Dict[str, Any] | None, mtime: float | None, size: int | None,
                    required_cache_key: str | None = None) -> bool:
    """
    Checks if the cached entry is valid based on mtime, size, and optional required key.
    """
    return (cached and mtime is not None and size is not None and
            cached.get("mtime") == mtime and cached.get("size") == size and
            (required_cache_key is None or (
                    required_cache_key in cached and cached.get(required_cache_key) is not None)))


def get_file_cover_hash(path: str, album_id: str | None = None) -> str | None:
    """
    Calculates the SHA256 hash of the embedded cover of the file.
    Uses a cache to avoid repeated calculations.
    Returns the hash as a hex string, _HASH_ERROR if there is a read error or None if there is no cover.
    """
    album_cache = album_file_hash_cache.setdefault(album_id if album_id else _NO_ALBUM_KEY, {})
    cached = album_cache.get(path)
    mtime, size = _get_file_stat(path)
    if _is_cache_valid(cached, mtime, size):
        return cached.get("hash")
    # Not cached or outdated - recalculate
    try:
        data = get_first_picture_bytes(path)
        h = hashlib.sha256(data).hexdigest() if data else None
    except IOError:
        h = _HASH_ERROR
    # Update cache only if file stats are valid
    if mtime is not None:
        current = album_cache.get(path, {})
        if current.get("hash") != h:
            # invalidate thumbnail_html
            current = {k: v for k, v in current.items() if k != "thumbnail_html"}
        album_cache[path] = {**current, "hash": h, "mtime": mtime, "size": size}
    return h

def build_mapping_from_cache(paths: list[str], album_id: str | None) -> dict:
    """
    Quickly builds the cover_status_to_files structure from the cache (calculates missing entries).
    """
    cover_status_to_files: Dict[str | None, List[str]] = {}
    for p in paths:
        h = get_file_cover_hash(p, album_id=album_id)
        if h == _HASH_ERROR:
            cover_status_to_files.setdefault(_HASH_ERROR, []).append(p)
        else:
            cover_status_to_files.setdefault(h, []).append(p)
    return cover_status_to_files

def _delayed_warn(album_id: str):
    if album_id not in _pending_warn_albums:
        return
    try:
        _pending_warn_albums.discard(album_id)
        files = file_list.get(album_id, {}).get("files", [])
        album_name = file_list.get(album_id, {}).get("name")
        mapping = build_mapping_from_cache(files, album_id)
        warn_if_multiple_covers(mapping, album_id=album_id, album_name=album_name)
    except (KeyError, AttributeError):
        log.error("%s: Error in delayed warn for album %s", PLUGIN_NAME, album_id)
        log.debug("%s: %s", PLUGIN_NAME, _traceback.format_exc())

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
            mapping.setdefault(_HASH_ERROR, []).append(path)
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
        elif h == _HASH_ERROR:
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

    def update_dialog(self, with_cover: QGroupBox, without_cover: QGroupBox, with_errors: QGroupBox):
        self.tab_with_cover.setLayout(QVBoxLayout())
        self.tab_with_cover.layout().addWidget(with_cover)
        self.tab_without_cover.setLayout(QVBoxLayout())
        self.tab_without_cover.layout().addWidget(without_cover)
        self.tab_with_errors.setLayout(QVBoxLayout())
        self.tab_with_errors.layout().addWidget(with_errors)

        try:
            self.show()
            self.raise_()
            self.activateWindow()
        except Exception:
            pass

    def _on_close_clicked(self):
        self.close()

def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.0f} {unit}"
        size /= 1024.0
    return f"{size:.0f} TB"

def _generate_thumbnail_html(img_data: bytes, album_id: str | None, idx: int) -> str:
    """
    Generate HTML for a thumbnail image or a fallback placeholder.
    Validates, scales, and encodes the image data.
    """
    if not PIL_AVAILABLE:
        return _THUMBNAIL_PIL_MISSING_HTML
    from PIL import Image, UnidentifiedImageError
    from io import BytesIO

    if len(img_data) > MAX_IMAGE_SIZE_BYTES:
        return (f'<div style="color: gray; font-style: italic;">Image data exceeds '
                f'{format_bytes(MAX_IMAGE_SIZE_BYTES)} limit for preview.</div>')

    img_io = BytesIO(img_data)
    try:
        img = Image.open(img_io)
        if img.size[0] > IMAGE_MAX_DIMENSION or img.size[1] > IMAGE_MAX_DIMENSION:
            img.close()
            raise ValueError(f"Image dimensions exceed maximum of {IMAGE_MAX_DIMENSION} pixels.")
        try:
            # Always create a copy first for independence
            final_img = img.copy()
            # Robust transparency detection
            has_transparency = (final_img.mode in ('RGBA', 'LA') or
                                (final_img.mode == 'P' and 'transparency' in final_img.info))
            try:
                if final_img.mode not in ('RGB', 'RGBA'):
                    new_img = final_img.convert('RGBA' if has_transparency else 'RGB')
                    final_img.close()
                    final_img = new_img
                # Now final_img is independent and valid
                final_img.thumbnail((THUMBNAIL_MAX_SIZE, THUMBNAIL_MAX_SIZE), Image.Resampling.LANCZOS)
                output = BytesIO()
                try:
                    final_img.save(output, format='PNG')
                    scaled_data = output.getvalue()
                    img_b64 = base64.b64encode(scaled_data).decode('utf-8')
                    return (f'<img src="data:image/png;base64,{img_b64}" alt="Cover preview" style="max-width:100px; '
                            f'max-height:100px; margin:5px;">')
                finally:
                    output.close()
            finally:
                final_img.close()
        finally:
            img.close()
    except UnidentifiedImageError:
        log.error("%s: Unidentified image format in album %s, cover group %d", PLUGIN_NAME, album_id or "unknown album",
                  idx)
        return '<div style="color: gray; font-style: italic;">Invalid image format.</div>'
    except ValueError as e:
        log.error("%s: %s in album %s, cover group %d", PLUGIN_NAME, str(e), album_id or "unknown album", idx)
        return '<div style="color: gray; font-style: italic;">Image dimensions too large.</div>'
    except OSError:
        log.error("%s: I/O error reading image in album %s, cover group %d", PLUGIN_NAME, album_id or "unknown album",
                  idx)
        return '<div style="color: gray; font-style: italic;">I/O error reading image.</div>'
    finally:
        img_io.close()

def get_cached_thumbnail_data(path: str, album_id: str | None, idx: int) -> str | None:
    """
    Returns cached base64-encoded thumbnail data or regenerates it if not cached or outdated.
    """
    album_cache = album_file_hash_cache.setdefault(album_id if album_id else _NO_ALBUM_KEY, {})
    cached = album_cache.get(path)
    mtime, size = _get_file_stat(path)
    if _is_cache_valid(cached, mtime, size, "thumbnail_data"):
        return cached["thumbnail_data"]
    # Not cached or outdated - generate new
    img_data = get_first_picture_bytes(path)
    h = get_file_cover_hash(path, album_id)
    if img_data:
        # Reuse thumbnail if hash is the same and thumbnail is cached
        if cached and "thumbnail_data" in cached and cached.get("hash") == h:
            thumbnail_data = cached["thumbnail_data"]
        else:
            thumbnail_data = _generate_thumbnail_data(img_data, album_id, idx)
    else:
        thumbnail_data = None

    # Update cache only if file stats are valid
    if mtime is not None:
        update_dict = {"mtime": mtime, "size": size, "thumbnail_data": thumbnail_data}
        if h is not None:
            update_dict["hash"] = h
        album_cache[path] = {**album_cache.get(path, {}), **update_dict}

    return thumbnail_data

def _generate_thumbnail_data(img_data: bytes, album_id: str | None, idx: int) -> str | None:
    """
    Generate base64-encoded thumbnail data or return None.
    Validates, scales, and encodes the image data.
    """
    if not PIL_AVAILABLE:
        return None
    from PIL import Image, UnidentifiedImageError
    from io import BytesIO

    if len(img_data) > MAX_IMAGE_SIZE_BYTES:
        return None

    img_io = BytesIO(img_data)
    try:
        img = Image.open(img_io)
        if img.size[0] > IMAGE_MAX_DIMENSION or img.size[1] > IMAGE_MAX_DIMENSION:
            img.close()
            raise ValueError(f"Image dimensions exceed maximum of {IMAGE_MAX_DIMENSION} pixels.")
        try:
            # Always create a copy first for independence
            final_img = img.copy()
            # Robust transparency detection
            has_transparency = (final_img.mode in ('RGBA', 'LA') or
                                (final_img.mode == 'P' and 'transparency' in final_img.info))
            try:
                if final_img.mode not in ('RGB', 'RGBA'):
                    new_img = final_img.convert('RGBA' if has_transparency else 'RGB')
                    final_img.close()
                    final_img = new_img
                # Now final_img is independent and valid
                final_img.thumbnail((THUMBNAIL_MAX_SIZE, THUMBNAIL_MAX_SIZE), Image.Resampling.LANCZOS)
                output = BytesIO()
                try:
                    final_img.save(output, format='PNG')
                    scaled_data = output.getvalue()
                    return base64.b64encode(scaled_data).decode('utf-8')
                finally:
                    output.close()
            finally:
                final_img.close()
        finally:
            img.close()
    except (UnidentifiedImageError, ValueError, OSError):
        log.error("%s: Error generating thumbnail data in album %s, cover group %d", PLUGIN_NAME,
                  album_id or "unknown album", idx)
        return None
    finally:
        img_io.close()

def prepare_aggregated_report(all_mappings: Dict[str, Dict]) -> tuple[QGroupBox, QGroupBox, QGroupBox]:
    for album_id, entry in all_mappings.items():
        mapping = entry.get("mapping") if isinstance(entry, dict) and "mapping" in entry else entry
        album_name = entry.get("name") if isinstance(entry, dict) else None

        real_hashes = [k for k in mapping.keys() if k not in (None, _HASH_ERROR)]
        distinct = set(real_hashes)
        different_covers = len(distinct) > 1 or (len(distinct) == 1 and None in mapping)
        if not different_covers:
            continue

        any_shown = True
        album_label = album_name or album_id

        # QGroupBox für jedes Album (einklappbar)
        cover_list = QGroupBox(f"Album: {album_label}")
        cover_list.setCheckable(True)
        cover_list.setChecked(False)
        album_layout = QVBoxLayout(cover_list)

        normal_groups = [(cover_hash, paths) for cover_hash, paths in mapping.items() if
                         cover_hash not in (None, _HASH_ERROR)]
        for idx, (h, paths) in enumerate(sorted(normal_groups, key=lambda x: -len(x[1])), start=1):
            row_layout = QHBoxLayout()

            # Thumbnail
            thumbnail_label = QLabel()
            thumbnail_data = get_cached_thumbnail_data(paths[0], album_id, idx)
            if thumbnail_data:
                pixmap = QPixmap()
                pixmap.loadFromData(base64.b64decode(thumbnail_data))
                if not pixmap.isNull():
                    thumbnail_label.setPixmap(pixmap.scaled(100, 100, Qt.KeepAspectRatio))
            row_layout.addWidget(thumbnail_label)

            # Liste der Dateinamen
            list_widget = QListWidget()
            for p in paths:
                list_widget.addItem(QListWidgetItem(os.path.basename(p)))
            row_layout.addWidget(list_widget)

            album_layout.addLayout(row_layout)

        # No Cover
        no_cover = mapping.get(None, [])
        no_cover_list = QListWidget()
        no_cover_list.addItem(QListWidgetItem(f"No embedded cover - {len(no_cover)} file(s):"))
        if no_cover:
            for p in no_cover:
                no_cover_list.addItem(QListWidgetItem(os.path.basename(p)))

        # Errors
        errors = mapping.get(_HASH_ERROR, [])
        errors_list = QListWidget()
        errors_list.addItem(QListWidgetItem(f"Read errors - {len(errors)} file(s):"))
        if errors:
            for p in errors:
                errors_list.addItem(QListWidgetItem(os.path.basename(p)))

    return cover_list, no_cover_list, errors_list

def warn_if_multiple_covers(mapping, parent_widget=None, album_id: str | None = None, album_name: str | None = None):
    """
    Non-blocking aggregated warning for all albums.
    - Updates internal per-album mapping and stored album name.
    - Maintains a single dialog for all albums
    """
    global all_albums_dialog, all_album_mappings, album_dialog_closed

    # compute whether this album has multiple covers
    real_hashes = [k for k in mapping.keys() if k not in (None, _HASH_ERROR)]
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
    with_cover, without_cover, errors = prepare_aggregated_report(filtered)

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
        all_albums_dialog.update_dialog(with_cover, without_cover, errors)
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
        real_hashes = [k for k in m.keys() if k not in (None, _HASH_ERROR)]
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

        if getattr(file, "filename", None):
            try:
                # prepopulate cache
                get_file_cover_hash(file.filename, album_id=album_id)
            except Exception:
                # swallow - error treated as _HASH_ERROR in the cache
                pass

        mapping = build_mapping_from_cache(file_list[album_id]["files"], album_id)

        for h, paths in mapping.items():
            log.debug("%s: cover=%s files=%s", PLUGIN_NAME, h, ", ".join(os.path.basename(p) for p in paths))

        # Do not immediately update UI for every file - Debounce
        if album_id not in _pending_warn_albums:
            _pending_warn_albums.add(album_id)
            QTimer.singleShot(_WARN_DEBOUNCE_MILLISECONDS,
                              lambda aid=album_id: _delayed_warn(aid))



    except Exception as e:
        log.error("%s: Error in protect_track_cover: %s", PLUGIN_NAME, e)
        log.error("%s: Traceback: %s", PLUGIN_NAME, _traceback.format_exc())



def on_album_removed(album: Any):
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
            log.warning("%s: Error removing dismissed signature for album %s", PLUGIN_NAME, album_id)

        # clear cache and pending flags
        try:
            album_file_hash_cache.pop(album_id, None)
            _pending_warn_albums.discard(album_id)
        except Exception:
            log.warning("%s: Error clearing cache for album %s", PLUGIN_NAME, album_id)

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
                    with_cover, without_cover, errors = prepare_aggregated_report(filtered)
                    all_albums_dialog.update_dialog(with_cover, without_cover, errors)
        except Exception:
            log.error("%s: Error updating dialog after album removal: %s", PLUGIN_NAME, _traceback.format_exc())

    except Exception:
        log.error("%s: Error in on_album_removed: %s", PLUGIN_NAME, _traceback.format_exc())

log.debug(PLUGIN_NAME + ": registration")
register_file_post_addition_to_track_processor(protect_track_cover)
register_album_post_removal_processor(on_album_removed)
