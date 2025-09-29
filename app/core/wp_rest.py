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

    def _ensure_term(self, tax: str, name: str):
        """指定taxonomyに term(name) が無ければ作成し、term ID を返す。
        - 既存の場合(400 term_exists)はそのIDを返す
        - name が不正/長すぎる場合はサニタイズして再試行
        """
        import requests
        nm = (name or "").strip()
        if not nm:
            return None

        # 1) まず検索で既存確認（完全一致を優先）
        r = self._req("GET", f"/wp-json/wp/v2/{tax}", params={"search": nm, "per_page": 100})
        data = r.json() if hasattr(r, "json") else r
        for t in (data or []):
            if t.get("name", "").lower() == nm.lower():
                return t.get("id")

        # 2) 作成を試行
        try:
            res = self._req("POST", f"/wp-json/wp/v2/{tax}", json={"name": nm})
            data = res.json() if hasattr(res, "json") else res
            return (data or {}).get("id")
        except requests.HTTPError as e:
            # WordPress は重複時 400 + {"code":"term_exists","data":{"term_id":…}}
            try:
                j = e.response.json()
                if j.get("code") == "term_exists":
                    return j.get("data", {}).get("term_id")
                # name が長すぎる/不正
                if j.get("code") in ("rest_invalid_param", "invalid_term"):
                    safe = nm[:190]  # WP は最大200程度。余裕を持って切る
                    if safe and safe != nm:
                        res2 = self._req("POST", f"/wp-json/wp/v2/{tax}", json={"name": safe})
                        data2 = res2.json() if hasattr(res2, "json") else res2
                        return (data2 or {}).get("id")
            except Exception:
                pass
            raise

    def ensure_tags(self, names: list[str]) -> list[int]:
        """タグ名リストから term IDs を取得/作成。空要素は除外し、重複IDも除去。"""
        ids, seen = [], set()
        for raw in names or []:
            nm = (raw or "").strip()
            if not nm:
                continue
            tid = self._ensure_term("tags", nm)
            if isinstance(tid, int) and tid not in seen:
                ids.append(tid); seen.add(tid)
        return ids

    def ensure_categories(self, names: list[str]) -> list[int]:
        """カテゴリ名リストから term IDs を取得/作成。空要素は除外し、重複IDも除去。"""
        ids, seen = [], set()
        for raw in names or []:
            nm = (raw or "").strip()
            if not nm:
                continue
            tid = self._ensure_term("categories", nm)
            if isinstance(tid, int) and tid not in seen:
                ids.append(tid); seen.add(tid)
        return ids

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
