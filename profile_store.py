# profile_store.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class DesktopProfile:
    name: str
    WP_URL: str = ""
    WP_USER: str = ""
    WP_APP_PASS: str = ""
    API_ID: str = ""
    FANZA_API_AFFILIATE_ID: str = ""
    FANZA_LINK_AFFILIATE_ID: str = ""

    @staticmethod
    def from_dict(name: str, d: dict) -> "DesktopProfile":
        return DesktopProfile(
            name=name,
            WP_URL=str(d.get("WP_URL", "") or ""),
            WP_USER=str(d.get("WP_USER", "") or ""),
            WP_APP_PASS=str(d.get("WP_APP_PASS", "") or ""),
            API_ID=str(d.get("API_ID", "") or ""),
            FANZA_API_AFFILIATE_ID=str(d.get("FANZA_API_AFFILIATE_ID", "") or ""),
            FANZA_LINK_AFFILIATE_ID=str(d.get("FANZA_LINK_AFFILIATE_ID", "") or ""),
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("name", None)
        return d


class ProfileStore:
    VERSION = 1

    def __init__(self, path: Optional[Path] = None):
        self.path = path or self._default_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._profiles: Dict[str, DesktopProfile] = {}
        self._last_selected: str = ""

    @staticmethod
    def _default_path() -> Path:
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "FANZAScraper" / "profiles.json"

        home = Path.home()
        if (home / "Library" / "Application Support").exists():
            return home / "Library" / "Application Support" / "FANZAScraper" / "profiles.json"

        xdg = os.environ.get("XDG_CONFIG_HOME")
        if xdg:
            return Path(xdg) / "FANZAScraper" / "profiles.json"
        return home / ".config" / "FANZAScraper" / "profiles.json"

    def load(self) -> None:
        if not self.path.exists():
            self._profiles = {}
            self._last_selected = ""
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if int(raw.get("version", 0)) != self.VERSION:
                self._profiles = {}
                self._last_selected = ""
                return
            profiles = raw.get("profiles", {}) or {}
            self._profiles = {str(n): DesktopProfile.from_dict(str(n), d or {}) for n, d in profiles.items()}
            self._last_selected = str(raw.get("last_selected", "") or "")
        except Exception:
            self._profiles = {}
            self._last_selected = ""

    def save(self) -> None:
        payload = {
            "version": self.VERSION,
            "last_selected": self._last_selected,
            "profiles": {k: v.to_dict() for k, v in self._profiles.items()},
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def list_names(self) -> List[str]:
        return sorted(self._profiles.keys(), key=lambda s: s.lower())

    def get(self, name: str) -> Optional[DesktopProfile]:
        return self._profiles.get(name)

    def upsert(self, profile: DesktopProfile) -> None:
        self._profiles[profile.name] = profile

    def delete(self, name: str) -> None:
        self._profiles.pop(name, None)
        if self._last_selected == name:
            self._last_selected = ""

    def set_last_selected(self, name: str) -> None:
        self._last_selected = name

    def last_selected(self) -> str:
        return self._last_selected
