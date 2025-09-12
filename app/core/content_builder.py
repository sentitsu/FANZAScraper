# app/core/content_builder.py
from pathlib import Path
from typing import Callable, Optional, Dict, Any
from jinja2 import Environment, FileSystemLoader, select_autoescape

try:
    import markdown2  # optional（.mdテンプレートをHTML化したい場合）
except Exception:
    markdown2 = None

class ContentBuilder:
    def __init__(
        self,
        template_path: Optional[str] = None,
        md_template_path: Optional[str] = None,
        prepend_html: Optional[str] = None,
        append_html: Optional[str] = None,
        hook: Optional[Callable[[Dict[str, Any], str], str]] = None,
    ):
        self.prepend_html = prepend_html or ""
        self.append_html = append_html or ""
        self.hook = hook

        self.env = None
        self.template = None
        self.md_template = None

        if template_path:
            tp = Path(template_path)
            self.env = Environment(
                loader=FileSystemLoader(str(tp.parent)),
                autoescape=select_autoescape(["html", "xml"])
            )
            self.template = self.env.get_template(tp.name)

        if md_template_path:
            if markdown2 is None:
                raise RuntimeError("markdown2 が必要です。`pip install markdown2` を実行してください。")
            mp = Path(md_template_path)
            self.md_env = Environment(loader=FileSystemLoader(str(mp.parent)))
            self.md_template = self.md_env.get_template(mp.name)

    def render(self, item: Dict[str, Any]) -> str:
        html_parts = []

        # 1) .mdテンプレート → HTML
        if self.md_template:
            md_text = self.md_template.render(item=item)
            html = markdown2.markdown(md_text)  # safe-modeは必要に応じて
            html_parts.append(html)

        # 2) .html.j2テンプレート
        if self.template:
            html = self.template.render(item=item)
            html_parts.append(html)

        # 3) prepend / append
        core_html = "".join(html_parts) if html_parts else ""
        final_html = f"{self.prepend_html}{core_html}{self.append_html}"

        # 4) 任意フック（Python関数）で最終加工
        if self.hook:
            final_html = self.hook(item, final_html)

        return final_html
