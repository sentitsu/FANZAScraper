# app/posting/wp_rest.py
import base64, time, requests
from typing import List, Tuple, Optional

class WPClient:
    def __init__(self, base_url: str, user: str, app_pass: str, timeout=30.0):
        self.base = base_url.rstrip("/")
        token = f"{user}:{app_pass}".encode("utf-8")
        self.headers = {
            "Authorization": "Basic " + base64.b64encode(token).decode("ascii"),
            "Content-Type": "application/json; charset=utf-8",
        }
        self.timeout = timeout

    def _req(self, method: str, path: str, **kw):
        url = f"{self.base}{path}"
        for i in range(5):
            r = requests.request(method, url, headers=self.headers, timeout=self.timeout, **kw)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (i + 1))
                continue
            r.raise_for_status()
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

    def find_post_id_by_external(self, external_id: str) -> Optional[int]:
        q = f"/wp-json/wp/v2/posts?meta_key=external_id&meta_value={requests.utils.quote(external_id)}&per_page=1&_fields=id"
        res = self._req("GET", q)
        if isinstance(res, list) and res:
            return res[0]["id"]
        return None

    def create_or_update_post(self, *, title: str, content: str, status: str,
                              categories: List[int], tags: List[int],
                              external_id: str, meta_extra: dict | None = None,
                              date: str | None = None) -> Tuple[int, str]:
        payload = {
            "title": title,
            "content": content,
            "status": status,
            "categories": categories or [],
            "tags": tags or [],
            "meta": {"external_id": external_id} | (meta_extra or {}),
        }
        if date and status == "future":
            payload["date"] = date  # ISO 8601

        pid = self.find_post_id_by_external(external_id)
        if pid:
            res = self._req("POST", f"/wp-json/wp/v2/posts/{pid}", json=payload)
        else:
            res = self._req("POST", "/wp-json/wp/v2/posts", json=payload)
        return res["id"], res.get("link", "")
