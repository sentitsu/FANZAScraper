# app/core/content_builder.py
import os
from pathlib import Path
from typing import Callable, Optional, Dict, Any, Tuple, List
from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateNotFound

try:
    import markdown2  # optional（.mdテンプレートをHTML化したい場合）
except Exception:
    markdown2 = None


def _search_roots() -> List[str]:
    """
    テンプレ探索の基準ディレクトリ候補を返す:
    - CWD（実行ディレクトリ）
    - CWD/templates
    - リポジトリ直下（app/ から2階層上を想定）
    - リポジトリ直下の templates/
    - このファイルと同階層の templates/
    """
    here = Path(__file__).resolve()
    app_root = here.parents[1]                 # .../app/core → .../app
    repo_root = app_root.parent                # .../repo
    return list(dict.fromkeys([
        str(Path.cwd()),
        str((Path.cwd() / "templates").resolve()),
        str(repo_root),
        str((repo_root / "templates").resolve()),
        str((here.parent / "templates").resolve()),
    ]))


def _make_env_and_name(path_like: Optional[str]) -> Tuple[Optional[Environment], Optional[str]]:
    """
    path_like が None → (None, None)
    - 絶対/相対パス: その親を searchpath に、name はファイル名
    - ファイル名だけ: 複数の search_roots + '/templates' で探索
    """
    if not path_like:
        return None, None

    p = Path(path_like)

    # 絶対 or ディレクトリ区切りを含む（相対パス）ならその親を使う
    if p.is_absolute() or any(sep in path_like for sep in (os.sep, "/")):
        p = p.expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"Template path not found: {p}")
        search_dirs = [str(p.parent)]
        name = p.name
        env = Environment(
            loader=FileSystemLoader(search_dirs),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return env, name

    # ファイル名だけ → 複数の候補ディレクトリ＋/templates を試す
    roots = _search_roots()
    # roots そのもの & roots/templates を両方試す
    search_dirs = []
    for r in roots:
        search_dirs.append(r)
        search_dirs.append(str((Path(r) / "templates").resolve()))
    # 重複除去
    search_dirs = list(dict.fromkeys(search_dirs))

    env = Environment(
        loader=FileSystemLoader(search_dirs),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    name = path_like
    return env, name


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
        self.md_env = None
        self.md_template = None

        # HTMLテンプレート
        if template_path:
            self.env, name = _make_env_and_name(template_path)
            try:
                self.template = self.env.get_template(name) if self.env else None
            except TemplateNotFound as e:
                raise TemplateNotFound(
                    f"HTML template '{name}' not found. searchpath={getattr(self.env.loader, 'searchpath', [])}"
                ) from e

        # Markdownテンプレート
        if md_template_path:
            if markdown2 is None:
                raise RuntimeError("markdown2 が必要です。`pip install markdown2` を実行してください。")
            self.md_env, md_name = _make_env_and_name(md_template_path)
            try:
                self.md_template = self.md_env.get_template(md_name) if self.md_env else None
            except TemplateNotFound as e:
                raise TemplateNotFound(
                    f"MD template '{md_name}' not found. searchpath={getattr(self.md_env.loader, 'searchpath', [])}"
                ) from e

    def render(self, item: Dict[str, Any]) -> str:
        html_parts = []

        # 1) .mdテンプレート → HTML
        if self.md_template:
            md_text = self.md_template.render(item=item)
            if markdown2 is None:
                raise RuntimeError("markdown2 が必要です。`pip install markdown2` を実行してください。")
            html_parts.append(markdown2.markdown(md_text))

        # 2) .html.j2テンプレート
        if self.template:
            html_parts.append(self.template.render(item=item))

        # 3) prepend / append
        core_html = "".join(html_parts) if html_parts else ""
        final_html = f"{self.prepend_html}{core_html}{self.append_html}"

        # 4) 任意フック（Python関数）で最終加工（例外は握りつぶさず伝える）
        if callable(self.hook):
            final_html = self.hook(item, final_html)

        return final_html
