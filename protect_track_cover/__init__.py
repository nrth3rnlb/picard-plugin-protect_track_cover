# -*- coding: utf-8 -*-

"""
Protect Track Cover Plugin for MusicBrainz Picard.
Warns if the tracks on an album use different covers
and there is a potential risk of these being replaced by the album cover.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from functools import partial
from typing import Any, List

from PyQt5 import uic
from PyQt5.QtCore import QObject, pyqtSignal
from PyQt5.QtWidgets import QLayout

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

import hashlib
import traceback as _traceback

from typing import Dict
from PyQt5.QtWidgets import QApplication, QWidget, QSizePolicy
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QVBoxLayout, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QGroupBox

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
MAX_VISIBLE_ROWS = 10
DEFAULT_ROW_HEIGHT = 24
ROW_PADDING = 4
UI_NO_CONTENT_MESSAGE = "Nothing to show."

MUSICBRAINZ_ALBUMID = "musicbrainz_albumid"

# Global cache: per-album file hash cache
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
    Make sure that `cached` is really a dict and return boolean.
    """
    if not isinstance(cached, dict):
        return False
    if mtime is None or size is None:
        return False
    if cached.get("mtime") != mtime or cached.get("size") != size:
        return False
    if required_cache_key is None:
        return True
    return required_cache_key in cached and cached.get(required_cache_key) is not None

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
        if not isinstance(current, dict):
            current = {}
        if current.get("hash") != h:
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
        for p in paths[:MAX_VISIBLE_ROWS]:
            parts.append(f"    {os.path.basename(p)}")
        if len(paths) > MAX_VISIBLE_ROWS:
            parts.append(f"    ... ({len(paths) - MAX_VISIBLE_ROWS} more)")
    return "\n".join(parts)

class UIUpdater(QObject):
    """
    Small QObject that provides a thread-safe signal/slot bridge:
    - request_update() can be called from any thread
    - _on_update() runs in the GUI thread and creates/updates widgets
    """
    updateRequested = pyqtSignal(dict, 'QWidget')

    def __init__(self, parent=None):
        # Ensure parent is passed to QObject to manage lifetime
        super().__init__(parent)
        self.updateRequested.connect(self._on_update)

    def request_update(self, filtered: dict, parent_widget=None) -> None:
        # emit is thread-safe; the connected slot is executed in the GUI thread
        try:
            self.updateRequested.emit(filtered, parent_widget)
        except Exception:
            log.error("%s: Failed to emit updateRequested: %s", PLUGIN_NAME, _traceback.format_exc())

    def _on_update(self, filtered: dict, parent_widget) -> None:
        """
        Slot, executed in the GUI thread: builds the GroupBoxes and updates the dialogue.
        """
        global all_albums_dialog
        try:
            with_cover, without_cover, errors = prepare_aggregated_report(filtered)

            app = QApplication.instance()
            created_app = False
            if app is None:
                app = QApplication([])
                created_app = True

            # Define a small handler that sets the module global to None when the dialog is destroyed.
            def _on_dialog_destroyed(*_):
                global all_albums_dialog
                all_albums_dialog = None

            if all_albums_dialog is None:
                all_albums_dialog = AlbumsWarningDialog(parent_widget)
                try:
                    all_albums_dialog.destroyed.connect(_on_dialog_destroyed)
                except Exception:
                    pass

            all_albums_dialog.update_dialog(with_cover, without_cover, errors)

            if created_app:
                # Temporarily created QApplication for headless operation; no further action required
                pass

        except Exception as e:
            log.error("%s: UI update failed: %s", PLUGIN_NAME, e)
            log.error("%s: Traceback: %s", PLUGIN_NAME, _traceback.format_exc())


from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QDialog, QStyle


class AlbumsWarningDialog(QDialog):
    """
    Dialog to warn about albums with multiple different track covers.

    Uses functools.partial for toggled handlers and stores handler references
    so they can be disconnected when the layout is cleared. Child widgets
    are queried with `findChildren()` each toggle to avoid stale references.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        ui_file = os.path.join(os.path.dirname(__file__), 'albums_warning.ui')
        uic.loadUi(ui_file, self)

        # storage for handler references keyed by id(groupbox)
        self._groupbox_handlers: dict[int, Callable[[bool], None]] = {}

        # Set dialog properties: non-modal and deleted on close
        self.setModal(False)
        self.setAttribute(Qt.WA_DeleteOnClose, True)
        self.closeButton.clicked.connect(self._on_close_clicked)

        # Try theme icon first, then fall back to platform standard icon
        icon = QIcon.fromTheme("dialog-warning")
        if icon.isNull():
            icon = self.style().standardIcon(QStyle.SP_MessageBoxWarning)

        size = self.iconLabel.maximumSize()
        if size.width() <= 0 or size.height() <= 0:
            pixmap = icon.pixmap(48, 48)
        else:
            pixmap = icon.pixmap(size.width(), size.height())

        self.iconLabel.setPixmap(pixmap)
        self.iconLabel.setScaledContents(False)
        self.tabs.setCurrentIndex(0)

    def _clear_layout(self, layout: QLayout) -> None:
        """Remove and delete all widgets from the given layout.
        Disconnect any stored groupbox handlers before deleting the widgets.
        """
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget:
                # If this is a QGroupBox we may have a stored handler to disconnect.
                try:
                    if isinstance(widget, QGroupBox):
                        handler = self._groupbox_handlers.pop(id(widget), None)
                        if handler is not None:
                            try:
                                widget.toggled.disconnect(handler)
                            except Exception:
                                # ignore disconnect failures
                                pass
                except Exception:
                    pass

                widget.setParent(None)
                widget.deleteLater()

    def _toggle_children(self, box: QGroupBox, checked: bool) -> None:
        """Toggle visibility of all descendant QWidget children of the given groupbox."""
        try:
            children = box.findChildren(QWidget)
            for child in children:
                try:
                    child.setVisible(checked)
                except Exception:
                    pass
        except Exception:
            pass

    def _add_groupboxes_to_layout(self, groupboxes: list[QGroupBox], target_layout: QLayout) -> None:
        """
        Add provided QGroupBox widgets to the target layout.
        - Uses a single instance method `_toggle_children` with `partial` to avoid
          per-iteration function definitions.
        - Stores handler references so they can be disconnected on cleanup.
        """
        for box in groupboxes:
            try:
                box.setCheckable(True)
                box.setChecked(True)
            except Exception:
                log.warning("%s: Failed setting GroupBox checkable state.", PLUGIN_NAME)

            # Create a handler using partial(self._toggle_children, box)
            try:
                handler = partial(self._toggle_children, box)
                box.toggled.connect(handler)
                # remember handler so we can disconnect later
                self._groupbox_handlers[id(box)] = handler
            except Exception:
                log.warning("%s: Failed connecting GroupBox toggled handler.", PLUGIN_NAME)

            # Set initial visibility based on current checked state
            try:
                checked = box.isChecked()
            except Exception:
                checked = True

            try:
                for child in box.findChildren(QWidget):
                    try:
                        child.setVisible(checked)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                target_layout.addWidget(box)
            except Exception:
                pass

    def update_dialog(self, with_cover: list[QGroupBox], without_cover: list[QGroupBox], with_errors: list[QGroupBox]):
        """
        Refresh dialog contents:
        - clear existing layouts (disconnect handlers first)
        - insert new groupboxes and ensure their children are correctly shown/hidden
        """
        self._clear_layout(self.layout_with_cover)
        self._clear_layout(self.layout_without_cover)
        self._clear_layout(self.layout_with_errors)

        self._add_groupboxes_to_layout(with_cover, self.layout_with_cover)
        self._add_groupboxes_to_layout(without_cover, self.layout_without_cover)
        self._add_groupboxes_to_layout(with_errors, self.layout_with_errors)

        try:
            self.show()
            self.raise_()
            self.activateWindow()
        except Exception:
            # Silently ignore platform-specific activation issues
            pass

    def _on_close_clicked(self):
        """Close the dialog when the close button is pressed."""
        self.close()


def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.0f} {unit}"
        size /= 1024.0
    return f"{size:.0f} TB"

def get_cached_thumbnail_data(path: str, album_id: str | None, idx: int) -> str | None:
    """
    Returns cached base64-encoded thumbnail data or regenerates it if not cached or outdated.
    """
    album_cache = album_file_hash_cache.setdefault(album_id if album_id else _NO_ALBUM_KEY, {})
    cached = album_cache.get(path)
    mtime, size = _get_file_stat(path)
    if _is_cache_valid(cached, mtime, size, "thumbnail_data"):
        if isinstance(cached, dict):
            return cached.get("thumbnail_data")
        return None
    # Not cached or outdated - generate new
    img_data = get_first_picture_bytes(path)
    h = get_file_cover_hash(path, album_id)
    if img_data:
        # Reuse thumbnail if hash is the same and thumbnail is cached
        if isinstance(cached, dict) and "thumbnail_data" in cached and cached.get("hash") == h:
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
        existing = album_cache.get(path)
        if not isinstance(existing, dict):
            existing = {}
        album_cache[path] = {**existing, **update_dict}

    return thumbnail_data

from PyQt5.QtGui import QImage, QPixmap
from PyQt5.QtCore import QByteArray, QBuffer, QIODevice
import base64

def _generate_thumbnail_data(img_data: bytes, album_id: str | None, idx: int) -> str | None:
    """
    Generate base64-encoded thumbnail data using Qt or return None.
    Validates, scales, and encodes the image data.
    """
    if len(img_data) > MAX_IMAGE_SIZE_BYTES:
        return None

    try:
        # Load image from bytes using QImage
        image = QImage.fromData(img_data)
        if image.isNull():
            raise ValueError("Invalid image data")

        # Check dimensions
        if image.width() > IMAGE_MAX_DIMENSION or image.height() > IMAGE_MAX_DIMENSION:
            raise ValueError(f"Image dimensions exceed maximum of {IMAGE_MAX_DIMENSION} pixels.")

        # Scale the image
        scaled_image = image.scaled(THUMBNAIL_MAX_SIZE, THUMBNAIL_MAX_SIZE, Qt.KeepAspectRatio, Qt.SmoothTransformation)

        # Save as PNG to QByteArray
        byte_array = QByteArray()
        buffer = QBuffer(byte_array)
        buffer.open(QIODevice.WriteOnly)
        if not scaled_image.save(buffer, "PNG"):
            raise ValueError("Failed to save image as PNG")
        buffer.close()

        # Encode to base64
        return base64.b64encode(byte_array.data()).decode('utf-8')

    except (ValueError, Exception) as e:
        log.error("%s: Error generating thumbnail data in album %s, cover group %d: %s", PLUGIN_NAME,
                  album_id or "unknown album", idx, str(e))
        return None

def create_track_list(min_height: int = THUMBNAIL_MAX_SIZE) -> QListWidget:
    """Creates a preconfigured QListWidget (minimum height, sorting)."""
    lst = QListWidget()
    lst.setSortingEnabled(True)
    lst.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)  # Vertical only as high as necessary
    lst.setMinimumHeight(min_height)  # at least as high as the thumbnail
    return lst

def adjust_list_height(lst: QListWidget, min_height: int = THUMBNAIL_MAX_SIZE,
                       max_visible_rows: int = MAX_VISIBLE_ROWS) -> None:
    """
    Call after filling the list. Calculates the required height from the row heights
    and sets the maximum/fixed height so that the list is only as large as necessary
    (but at least `min_height`).
    """
    rows = lst.count()
    if rows <= 0:
        lst.setFixedHeight(min_height)
        return

    # Safely compute row height once (guard against widget/model errors)
    row_h = min_height  # default fallback
    try:
        if lst.count() > 0:  # fresh check to avoid race condition
            first_hint = lst.sizeHintForRow(0)
            if first_hint > 0:
                row_h = first_hint
            else:
                row_h = lst.sizeHintForIndex(lst.model().index(0, 0)).height()
    except Exception:
        # Use font metrics height as a more reasonable fallback for text-based lists
        try:
            row_h = lst.fontMetrics().height() + ROW_PADDING  # add small padding
        except Exception:
            row_h = DEFAULT_ROW_HEIGHT  # fixed fallback if font metrics fail
        log.warning("%s: Failed to determine QListWidget row height, using font-based fallback.", PLUGIN_NAME)

    frame = 2 * lst.frameWidth()
    desired = row_h * rows + frame
    # Minimum thumbnail size, maximum n visible rows (with scrollbar)
    desired = max(desired, min_height)
    desired = min(desired, row_h * max_visible_rows + frame)

    lst.setFixedHeight(desired)

def prepare_aggregated_report(all_mappings: Dict[str, Dict]) -> tuple[
    list[QGroupBox], list[QGroupBox], list[QGroupBox]]:
    """
    Prepares the aggregated report as lists of QGroupBox widgets for:
    - tracks with covers (grouped by hash)
    - tracks without covers
    - tracks with read errors
    """
    with_cover: list[QGroupBox] = []
    without_cover: list[QGroupBox] = []
    with_errors: list[QGroupBox] = []

    for album_id, entry in all_mappings.items():
        mapping = entry.get("mapping") if isinstance(entry, dict) and "mapping" in entry else entry
        if mapping is None or not isinstance(mapping, dict):
            mapping = {}
        if not get_different_covers(mapping):
            continue

        album_name = entry.get("name") if isinstance(entry, dict) else None
        album_label = album_name or album_id

        # -- Group: tracks with covers (grouped by hash) --
        groupbox_with_cover = QGroupBox(f"{album_label}")
        layout_with_cover = QVBoxLayout(groupbox_with_cover)
        layout_with_cover.setAlignment(Qt.AlignTop)
        layout_with_cover.setSizeConstraint(QLayout.SetMinimumSize)
        groupbox_with_cover.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        with_cover.append(groupbox_with_cover)

        cover_groups = [(cover_hash, paths) for cover_hash, paths in mapping.items()
                        if cover_hash not in (None, _HASH_ERROR)]

        if not cover_groups:
            layout_with_cover.addWidget(QLabel(UI_NO_CONTENT_MESSAGE))
        else:
            for idx, (h, paths) in enumerate(sorted(cover_groups, key=lambda x: -len(x[1])), start=1):
                item_widget = QWidget()
                hl = QHBoxLayout(item_widget)
                hl.setAlignment(Qt.AlignTop)
                item_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)

                # Thumbnail (feste Größe)
                thumbnail_label = QLabel()
                thumbnail_label.setAlignment(Qt.AlignTop)
                thumbnail_label.setFixedSize(THUMBNAIL_MAX_SIZE, THUMBNAIL_MAX_SIZE)
                thumbnail_data = get_cached_thumbnail_data(paths[0], album_id, idx)
                if thumbnail_data:
                    pixmap = QPixmap()
                    pixmap.loadFromData(base64.b64decode(thumbnail_data))
                    if not pixmap.isNull():
                        thumbnail_label.setPixmap(
                            pixmap.scaled(THUMBNAIL_MAX_SIZE, THUMBNAIL_MAX_SIZE, Qt.KeepAspectRatio,
                                          Qt.SmoothTransformation))
                hl.addWidget(thumbnail_label)

                # Track list
                track_list = create_track_list()
                for p in paths:
                    track_list.addItem(QListWidgetItem(os.path.basename(p)))
                adjust_list_height(track_list)
                hl.addWidget(track_list)

                layout_with_cover.addWidget(item_widget)

        # -- Group: tracks without covers (list of paths) --
        groupbox_without_cover = QGroupBox(f"{album_label}")
        layout_without = QVBoxLayout(groupbox_without_cover)
        layout_without.setAlignment(Qt.AlignTop)
        layout_without.setSizeConstraint(QLayout.SetMinimumSize)
        groupbox_without_cover.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        without_cover.append(groupbox_without_cover)

        no_cover_paths = mapping.get(None, [])
        if not no_cover_paths:
            layout_without.addWidget(QLabel(UI_NO_CONTENT_MESSAGE))
        else:
            # Sort once for display
            sorted_no_cover_paths = sorted(no_cover_paths)
            item_widget = QWidget()
            hl = QHBoxLayout(item_widget)
            hl.setAlignment(Qt.AlignTop)
            item_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)

            track_list = create_track_list()
            for p in sorted_no_cover_paths:
                track_list.addItem(QListWidgetItem(os.path.basename(p)))
            adjust_list_height(track_list)
            hl.addWidget(track_list)

            layout_without.addWidget(item_widget)

        # -- Group: tracks with read errors --
        groupbox_errors = QGroupBox(f"{album_label}")
        layout_errors = QVBoxLayout(groupbox_errors)
        layout_errors.setAlignment(Qt.AlignTop)
        layout_errors.setSizeConstraint(QLayout.SetMinimumSize)
        groupbox_errors.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        with_errors.append(groupbox_errors)

        error_paths = mapping.get(_HASH_ERROR, [])
        if not error_paths:
            layout_errors.addWidget(QLabel(UI_NO_CONTENT_MESSAGE))
        else:
            # Sort once for display
            sorted_error_paths = sorted(error_paths)
            item_widget = QWidget()
            hl = QHBoxLayout(item_widget)
            hl.setAlignment(Qt.AlignTop)
            item_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)

            track_list = create_track_list()
            for p in sorted_error_paths:
                track_list.addItem(QListWidgetItem(os.path.basename(p)))
            adjust_list_height(track_list)
            hl.addWidget(track_list)

            layout_errors.addWidget(item_widget)

    return with_cover, without_cover, with_errors

def get_different_covers(mapping: dict[Any, Any] | None = None) -> bool:
    """
    Determines if the mapping indicates multiple different covers.

    Accepts an explicit mapping or None. Non-dict values are treated as empty mapping.
    Returns True if there are different covers, False otherwise.
    """
    if mapping is None or not isinstance(mapping, dict):
        mapping = {}

    real_hashes = [k for k in mapping.keys() if k not in (None, _HASH_ERROR)]
    distinct = set(real_hashes)
    return len(distinct) > 1 or (len(distinct) == 1 and None in mapping)

ui_updater: UIUpdater | None = None

def get_ui_updater(parent_widget=None) -> UIUpdater:
    """
    Return the module-level UIUpdater instance, creating it if necessary.
    Prefer to set QApplication.instance() (or the provided parent_widget if it is a QObject)
    as the parent to ensure the updater is owned by the Qt object tree and not
    garbage collected prematurely.
    """
    global ui_updater
    if ui_updater is not None:
        return ui_updater

    parent = None
    try:
        # prefer a sensible QObject parent if available
        if parent_widget is not None and isinstance(parent_widget, QObject):
            parent = parent_widget
        else:
            app = QApplication.instance()
            if app is not None:
                parent = app
    except Exception:
        parent = None

    # create and store the updater with chosen parent (may be None if no app yet)
    ui_updater = UIUpdater(parent)
    return ui_updater

def warn_if_multiple_covers(mapping, parent_widget=None, album_id: str | None = None, album_name: str | None = None):
    """
    Non-blocking aggregated warning for all albums.
    Now delegates UI creation to ui_updater to ensure all widget work is on the GUI thread.
    """
    global all_albums_dialog, all_album_mappings, album_dialog_closed

    # compute whether this album has multiple covers
    different_covers = get_different_covers(mapping)

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
                # direct assignment instead of globals().update(...)
                all_albums_dialog = None
        except Exception:
            pass
        return True

    # Build filtered data only (no widgets!) and delegate UI work to ui_updater
    filtered = {aid: all_album_mappings[aid] for aid in albums_to_show}
    # use get_ui_updater to ensure proper parent/lifetime
    get_ui_updater(parent_widget).request_update(filtered, parent_widget)
    return True

def get_albums_to_show(dialog_closed: set[str], album_mappings: dict[str, dict[str, Any]]) -> list[str]:
    albums_to_show = []
    for aid, entry in album_mappings.items():
        mapping = entry.get('mapping') if isinstance(entry, dict) and 'mapping' in entry else entry
        if mapping is None or not isinstance(mapping, dict):
            mapping = {}
        if get_different_covers(mapping) and aid not in dialog_closed:
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
    """Handles cleanup when an album is removed from the library."""
    global all_albums_dialog
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
                    # direct assignment instead of globals().update(...)
                    all_albums_dialog = None
            else:
                # build filtered data and delegate UI work to the UIUpdater (GUI thread)
                filtered = {aid: all_album_mappings[aid] for aid in albums_to_show}
                try:
                    # parent unknown here; pass None (UIUpdater will prefer QApplication.instance() as parent)
                    get_ui_updater(None).request_update(filtered, None)
                except Exception:
                    log.error("%s: Failed to request UI update after album removal: %s", PLUGIN_NAME,
                              _traceback.format_exc())
        except Exception:
            log.error("%s: Error updating dialog after album removal: %s", PLUGIN_NAME, _traceback.format_exc())

    except Exception:
        log.error("%s: Error in on_album_removed: %s", PLUGIN_NAME, _traceback.format_exc())

log.debug(PLUGIN_NAME + ": registration")
register_file_post_addition_to_track_processor(protect_track_cover)
register_album_post_removal_processor(on_album_removed)
