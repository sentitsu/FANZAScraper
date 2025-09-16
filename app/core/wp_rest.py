# app/core/wp_rest.py
import base64
import time
import requests
from typing import List, Tuple, Optional
import os, mimetypes
from urllib.parse import urlparse

class WPClient:
    def __init__(self, base_url: str, user: str, app_pass: str, timeout=30.0):
        self.base = base_url.rstrip("/")
        token = f"{user}:{app_pass}".encode("utf-8")
        self.auth = "Basic " + base64.b64encode(token).decode("ascii")
        self.headers_json = {
            "Authorization": self.auth,
            "Content-Type": "application/json; charset=utf-8",
        }
        self.timeout = timeout

    def _req(self, method: str, path: str, **kw):
        url = f"{self.base}{path}"
        headers = kw.pop("headers", {})
        # JSONエンドポイント用の既定ヘッダをマージ
        for k, v in self.headers_json.items():
            headers.setdefault(k, v)

        for i in range(5):
            r = requests.request(method, url, headers=headers, timeout=self.timeout, **kw)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (i + 1))
                continue
            r.raise_for_status()
            # 201/200 いずれも JSON を返す
            return r.json()

    def _ensure_term(self, tax: str, name: str) -> int:
        res = self._req("GET", f"/wp-json/wp/v2/{tax}?search={requests.utils.quote(name)}&per_page=1")
        if isinstance(res, list) and res and res[0]["name"].lower() == name.lower():
            return res[0]["id"]
        res = self._req("POST", f"/wp-json/wp/v2/{tax}", json={"name": name})
        return res["id"]

    def ensure_categories(self, names: List[str]) -> List[int]:
        return [self._ensure_term("categories", n.strip()) for n in names if n.strip()]

    def ensure_tags(self, names: List[str]) -> List[int]:
        return [self._ensure_term("tags", n.strip()) for n in names if n.strip()]

    from typing import Optional

    def find_post_id_by_external(self, external_id: str) -> Optional[int]:
        eid = (external_id or "").strip()
        if not eid:
            return None
        q = (
            "/wp-json/wp/v2/posts"
            f"?meta_key=external_id&meta_value={requests.utils.quote(eid)}"
            "&per_page=10"
                "&status=publish,draft,future,pending,private"
            "&context=edit"
            "&_fields=id,meta,modified"
        )
        res = self._req("GET", q)
        if not isinstance(res, list):
            return None
        for p in res:
            if (p.get("meta") or {}).get("external_id") == eid:  # ← 厳密一致
                return p["id"]
        return None


    def upload_media_from_url(self, url: str, filename: str | None = None) -> int:
        """
        画像URLをGET→ /wp-json/wp/v2/media へ multipart でアップロードして添付IDを返す。
        """
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()

        path = urlparse(url).path
        name = filename or (os.path.basename(path) or "image.jpg")
        ctype = mimetypes.guess_type(name)[0] or "image/jpeg"

        files = {"file": (name, resp.content, ctype)}
        # multipart では Content-Type を自前で付けない（requests が boundary を付与）
        headers = {"Authorization": self.auth}
        r = requests.post(f"{self.base}/wp-json/wp/v2/media",
                          headers=headers, files=files, timeout=self.timeout)
        r.raise_for_status()
        return r.json()["id"]

    def create_or_update_post(
        self, *,
        title: str, content: str, status: str,
        categories: List[int], tags: List[int],
        external_id: str, meta_extra: dict | None = None,
        excerpt: Optional[str] = None,
        date: str | None = None,
        featured_media: Optional[int] = None
    ) -> Tuple[int, str]:
        payload = {
            "title": title,
            "content": content,
            "status": status,
            "categories": categories,
            "tags": tags,
            "meta": {"external_id": external_id, **(meta_extra or {})},
        }
        if excerpt:
            payload["excerpt"] = excerpt
        if date:
            payload["date"] = date
        if featured_media:
            payload["featured_media"] = featured_media

        pid = self.find_post_id_by_external(external_id)
        if pid:
            res = self._req("POST", f"/wp-json/wp/v2/posts/{pid}", json=payload)
        else:
            res = self._req("POST", "/wp-json/wp/v2/posts", json=payload)
        return res["id"], res.get("link", "")
