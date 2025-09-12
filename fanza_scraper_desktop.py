#!/usr/bin/env python3
"""
FANZA Scraper Desktop – Tkinter GUI (v2)

既存の CLI (`python -m app.main ...`) をサブプロセス実行するGUI。
今回の更新で、CLIに追加した全オプションをGUIから指定可能にしました：
- 取得系: site/service/floor/keyword/cid/gte-date/lte-date/sort/hits/max
- 画像/品質: verify-images/min-samples/skip-placeholder/no-content
- ネットワークHEAD制御: no-head-check/head-timeout/head-insecure
- 内容フィルタ: include/exclude (maker/actress/genre/title/cid-prefix)
- WordPress: wp-post/wp-url/wp-user/wp-app-pass/wp-categories/wp-tags/publish/future-datetime
- 出力: outfile

ビルド（単一exe）：
    pip install -U pyinstaller
    pyinstaller --noconfirm --onefile --windowed fanza_scraper_desktop.py
"""

import os
import sys
import shlex
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

APP_TITLE = "FANZA Scraper Desktop"

# ------------------ Helpers ------------------
def add_labeled_entry(parent, label, textvar, width=0, show=None, placeholder=None):
    row = ttk.Frame(parent)
    row.pack(fill=tk.X, padx=8, pady=4)
    ttk.Label(row, text=label, width=20, anchor=tk.W).pack(side=tk.LEFT)
    ent = ttk.Entry(row, textvariable=textvar, show=show)
    if width:
        ent.config(width=width)
    ent.pack(side=tk.LEFT, fill=tk.X, expand=True)
    # 既に値が入っているときは placeholder を挿入しない（FANZAFANZA バグ対策）
    if placeholder and not (textvar.get() or "").strip():
        ent.insert(0, placeholder)
    return ent

class ProcessRunner:
    def __init__(self, on_line, on_exit):
        self.on_line = on_line
        self.on_exit = on_exit
        self.proc = None
        self.thread = None
        self._stop = threading.Event()

    def run(self, cmd, cwd=None, env=None):
        if self.proc is not None:
            raise RuntimeError("Process already running")
        self.proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.thread = threading.Thread(target=self._pump, daemon=True)
        self.thread.start()

    def _pump(self):
        try:
            assert self.proc and self.proc.stdout
            for line in self.proc.stdout:
                if self._stop.is_set():
                    break
                self.on_line(line.rstrip("\n"))
        finally:
            code = None
            if self.proc:
                self.proc.wait()
                code = self.proc.returncode
            self.on_exit(code)
            self.proc = None
            self.thread = None

    def kill(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        self._stop.set()

class App(tk.Tk):
    def _load_env(self):
        # exe なら exe のあるフォルダ、開発時は このファイルのフォルダ と CWD を順に探す
        candidates = []
        if getattr(sys, "frozen", False):
            candidates.append(Path(sys.argv[0]).parent / ".env")
        candidates += [Path(__file__).parent / ".env", Path.cwd() / ".env"]
        for p in candidates:
            try:
                if p.exists():
                    load_dotenv(p)  # 見つかった .env をロード
                    break
            except Exception:
                pass

    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("980x720")
        self.minsize(940, 680)

        self._load_env()
        self.runner = ProcessRunner(self._on_proc_line, self._on_proc_exit)
        self._build_ui()

    def _browse_outfile(self):
        from tkinter import filedialog
        path = filedialog.asksaveasfilename(
            title="出力CSVの保存先",
            defaultextension=".csv",
            filetypes=[("CSV", ".csv"), ("All Files", "*.*")],
            initialfile=os.path.basename(self.var_outfile.get()) if hasattr(self, "var_outfile") else "output.csv",
        )
        if path:
            self.var_outfile.set(path)

    # -------------- UI --------------
    def _build_ui(self):
        pad = 8
        root = ttk.Frame(self)
        root.pack(fill=tk.BOTH, expand=True, padx=pad, pady=pad)

        nb = ttk.Notebook(root)
        nb.pack(fill=tk.BOTH, expand=True)

        # Tabs
        tab_basic = ttk.Frame(nb)
        tab_filters = ttk.Frame(nb)
        tab_content = ttk.Frame(nb)   # ★ 本文タブを追加
        tab_wp = ttk.Frame(nb)
        tab_advanced = ttk.Frame(nb)
        nb.add(tab_basic, text="基本")
        nb.add(tab_filters, text="フィルタ")
        nb.add(tab_content, text="本文")   # ★ 追加
        nb.add(tab_wp, text="WordPress")
        nb.add(tab_advanced, text="高度")

        # ---------- 基本 ----------
        self.var_site = tk.StringVar(value="FANZA")
        self.var_service = tk.StringVar(value="digital")
        self.var_floor = tk.StringVar(value="videoa")
        self.var_keyword = tk.StringVar()
        self.var_cid = tk.StringVar()
        self.var_gte = tk.StringVar()
        self.var_lte = tk.StringVar()

        topA = ttk.LabelFrame(tab_basic, text="取得条件")
        topA.pack(fill=tk.X, padx=6, pady=6)
        add_labeled_entry(topA, "site", self.var_site)
        add_labeled_entry(topA, "service", self.var_service)
        add_labeled_entry(topA, "floor", self.var_floor)
        add_labeled_entry(topA, "keyword", self.var_keyword, placeholder="例: 単体女優")
        add_labeled_entry(topA, "cid", self.var_cid, placeholder="例: SSIS-123")
        add_labeled_entry(topA, "gte-date", self.var_gte, placeholder="YYYY-MM-DD")
        add_labeled_entry(topA, "lte-date", self.var_lte, placeholder="YYYY-MM-DD")

        self.var_hits = tk.IntVar(value=20)
        self.var_max = tk.IntVar(value=20)
        self.var_sort = tk.StringVar(value="date")

        topB = ttk.LabelFrame(tab_basic, text="件数・出力")
        topB.pack(fill=tk.X, padx=6, pady=6)
        row1 = ttk.Frame(topB); row1.pack(fill=tk.X, pady=4)
        ttk.Label(row1, text="ヒット数 (--hits)").pack(side=tk.LEFT)
        ttk.Spinbox(row1, from_=1, to=100, textvariable=self.var_hits, width=8).pack(side=tk.LEFT, padx=(6, 18))
        ttk.Label(row1, text="最大件数 (--max)").pack(side=tk.LEFT)
        ttk.Spinbox(row1, from_=1, to=10000, textvariable=self.var_max, width=10).pack(side=tk.LEFT, padx=(6, 18))
        ttk.Label(row1, text="ソート (--sort)").pack(side=tk.LEFT)
        ttk.Combobox(row1, textvariable=self.var_sort, state="readonly",
                     values=["date","-date","rank","-rank","price","-price"], width=10).pack(side=tk.LEFT, padx=(6, 18))

        self.var_outfile = tk.StringVar(value=os.path.join(os.getcwd(), "out", f"fanza_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"))
        row2 = ttk.Frame(topB); row2.pack(fill=tk.X, pady=4)
        ttk.Label(row2, text="出力CSV (--outfile)").pack(side=tk.LEFT)
        ent_out = ttk.Entry(row2, textvariable=self.var_outfile)
        ent_out.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 6))
        ttk.Button(row2, text="参照...", command=self._browse_outfile).pack(side=tk.LEFT)

        self.var_verify = tk.BooleanVar(value=True)
        self.var_no_content = tk.BooleanVar(value=False)
        self.var_min_samples = tk.IntVar(value=1)
        self.var_skip_placeholder = tk.BooleanVar(value=False)

        topC = ttk.LabelFrame(tab_basic, text="画像・品質")
        topC.pack(fill=tk.X, padx=6, pady=6)
        ttk.Checkbutton(topC, text="画像検証 (--verify-images)", variable=self.var_verify).pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(topC, text="本文生成なし (--no-content)", variable=self.var_no_content).pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(topC, text="プレースホルダ除外 (--skip-placeholder)", variable=self.var_skip_placeholder).pack(side=tk.LEFT, padx=6)
        ttk.Label(topC, text="最小サンプル枚数 (--min-samples)").pack(side=tk.LEFT, padx=(12, 6))
        ttk.Spinbox(topC, from_=0, to=50, textvariable=self.var_min_samples, width=6).pack(side=tk.LEFT)

        # ---------- フィルタ ----------
        self.var_inc_maker = tk.StringVar()
        self.var_exc_maker = tk.StringVar()
        self.var_inc_actress = tk.StringVar()
        self.var_exc_actress = tk.StringVar()
        self.var_inc_genre = tk.StringVar()
        self.var_exc_genre = tk.StringVar()
        self.var_inc_title = tk.StringVar()
        self.var_exc_title = tk.StringVar()
        self.var_inc_cidp = tk.StringVar()
        self.var_exc_cidp = tk.StringVar()

        f1 = ttk.LabelFrame(tab_filters, text="include / exclude（正規表現 | 区切りもOK）")
        f1.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        add_labeled_entry(f1, "include-maker", self.var_inc_maker, placeholder="例: S1|MOODYZ")
        add_labeled_entry(f1, "exclude-maker", self.var_exc_maker)
        add_labeled_entry(f1, "include-actress", self.var_inc_actress, placeholder="例: 三上|葵")
        add_labeled_entry(f1, "exclude-actress", self.var_exc_actress)
        add_labeled_entry(f1, "include-genre", self.var_inc_genre, placeholder="例: 単体|専属")
        add_labeled_entry(f1, "exclude-genre", self.var_exc_genre, placeholder="例: 企画|オムニバス|VR")
        add_labeled_entry(f1, "include-title", self.var_inc_title, placeholder="例: デビュー|初撮り")
        add_labeled_entry(f1, "exclude-title", self.var_exc_title)
        add_labeled_entry(f1, "include-cid-prefix", self.var_inc_cidp, placeholder="例: ^SSIS|^ABW")
        add_labeled_entry(f1, "exclude-cid-prefix", self.var_exc_cidp)

        # ---------- 本文（テンプレ／フック） ----------
        self.var_ctmpl = tk.StringVar()
        self.var_cmdtmpl = tk.StringVar()
        self.var_pre = tk.StringVar()
        self.var_post = tk.StringVar()
        self.var_hook = tk.StringVar()
        self.var_maxgal = tk.IntVar(value=12)

        cf = ttk.LabelFrame(tab_content, text="テンプレート / フック")
        cf.pack(fill=tk.X, padx=6, pady=6)

        def _pick(var, title, patterns):
            path = filedialog.askopenfilename(title=title, filetypes=patterns)
            if path:
                var.set(path)

        row_ct = ttk.Frame(cf); row_ct.pack(fill=tk.X, pady=4)
        ttk.Label(row_ct, text="content-template", width=20).pack(side=tk.LEFT)
        ttk.Entry(row_ct, textvariable=self.var_ctmpl).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(row_ct, text="参照…", command=lambda: _pick(self.var_ctmpl, "content-template", [["Jinja2/HTML", ".j2 .html"], ["All", "*.*"]])).pack(side=tk.LEFT)

        row_md = ttk.Frame(cf); row_md.pack(fill=tk.X, pady=4)
        ttk.Label(row_md, text="content-md-template", width=20).pack(side=tk.LEFT)
        ttk.Entry(row_md, textvariable=self.var_cmdtmpl).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Button(row_md, text="参照…", command=lambda: _pick(self.var_cmdtmpl, "content-md-template", [["Jinja2/Markdown", ".j2 .md"], ["All", "*.*"]])).pack(side=tk.LEFT)

        row_pre = ttk.Frame(cf); row_pre.pack(fill=tk.X, pady=4)
        ttk.Label(row_pre, text="prepend-html", width=20).pack(side=tk.LEFT)
        ttk.Entry(row_pre, textvariable=self.var_pre).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)

        row_post = ttk.Frame(cf); row_post.pack(fill=tk.X, pady=4)
        ttk.Label(row_post, text="append-html", width=20).pack(side=tk.LEFT)
        ttk.Entry(row_post, textvariable=self.var_post).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)

        row_hook = ttk.Frame(cf); row_hook.pack(fill=tk.X, pady=4)
        ttk.Label(row_hook, text="content-hook", width=20).pack(side=tk.LEFT)
        ttk.Entry(row_hook, textvariable=self.var_hook).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6)
        ttk.Label(row_hook, text="例: hooks.myhook:transform").pack(side=tk.LEFT, padx=6)

        row_g = ttk.Frame(cf); row_g.pack(fill=tk.X, pady=4)
        ttk.Label(row_g, text="max-gallery", width=20).pack(side=tk.LEFT)
        ttk.Spinbox(row_g, from_=0, to=60, textvariable=self.var_maxgal, width=6).pack(side=tk.LEFT)

        # ---------- WordPress ----------
        self.var_wp_post = tk.BooleanVar(value=False)
        self.var_wp_url = tk.StringVar(value=os.getenv("WP_URL", ""))
        self.var_wp_user = tk.StringVar(value=os.getenv("WP_USER", ""))
        self.var_wp_app = tk.StringVar(value=os.getenv("WP_APP_PASS", ""))
        self.var_wp_cats = tk.StringVar(value=os.getenv("WP_CATEGORIES", ""))
        self.var_wp_tags = tk.StringVar(value=os.getenv("WP_TAGS", ""))
        self.var_publish = tk.BooleanVar(value=False)
        self.var_future = tk.StringVar()

        wpf = ttk.LabelFrame(tab_wp, text="WordPress 投稿設定")
        wpf.pack(fill=tk.X, padx=6, pady=6)
        ttk.Checkbutton(wpf, text="RESTで投稿する (--wp-post)", variable=self.var_wp_post).pack(fill=tk.X, padx=8, pady=4)
        add_labeled_entry(wpf, "WP_URL", self.var_wp_url)
        add_labeled_entry(wpf, "WP_USER", self.var_wp_user)
        add_labeled_entry(wpf, "WP_APP_PASS", self.var_wp_app, show="*")
        add_labeled_entry(wpf, "WP_CATEGORIES", self.var_wp_cats, placeholder="カンマ区切り")
        add_labeled_entry(wpf, "WP_TAGS", self.var_wp_tags, placeholder="カンマ区切り")
        ttk.Checkbutton(wpf, text="即時公開 (--publish)", variable=self.var_publish).pack(fill=tk.X, padx=8, pady=4)
        add_labeled_entry(wpf, "future-datetime", self.var_future, placeholder="ISO: 2025-09-11T21:00:00")

        # ---------- 高度 ----------
        self.var_no_head = tk.BooleanVar(value=False)
        self.var_head_timeout = tk.DoubleVar(value=3.0)
        self.var_head_insecure = tk.BooleanVar(value=False)

        adv = ttk.LabelFrame(tab_advanced, text="ネットワークHEAD判定")
        adv.pack(fill=tk.X, padx=6, pady=6)
        ttk.Checkbutton(adv, text="HEADを使わない (--no-head-check)", variable=self.var_no_head).pack(side=tk.LEFT, padx=6)
        ttk.Label(adv, text="timeout秒 (--head-timeout)").pack(side=tk.LEFT, padx=(12, 6))
        ttk.Spinbox(adv, from_=1, to=30, increment=0.5, textvariable=self.var_head_timeout, width=6).pack(side=tk.LEFT)
        ttk.Checkbutton(adv, text="SSL検証off (--head-insecure)", variable=self.var_head_insecure).pack(side=tk.LEFT, padx=12)

        # API/AFFILIATE
        self.var_api_id = tk.StringVar(value=os.getenv("API_ID", ""))
        self.var_affid = tk.StringVar(value=os.getenv("AFFILIATE_ID", ""))
        envf = ttk.LabelFrame(tab_advanced, text="APIキー（環境変数として注入）")
        envf.pack(fill=tk.X, padx=6, pady=6)
        add_labeled_entry(envf, "API_ID", self.var_api_id)
        add_labeled_entry(envf, "AFFILIATE_ID", self.var_affid)

        # Buttons + Log
        btns = ttk.Frame(root)
        btns.pack(fill=tk.X, pady=(8, 6))
        self.btn_run = ttk.Button(btns, text="実行", command=self._on_run)
        self.btn_run.pack(side=tk.LEFT)
        ttk.Button(btns, text="停止", command=self._on_kill).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(btns, text="クリア", command=self._clear_log).pack(side=tk.LEFT, padx=(8, 0))

        logf = ttk.LabelFrame(root, text="ログ")
        logf.pack(fill=tk.BOTH, expand=True)
        self.txt = tk.Text(logf, wrap=tk.NONE, height=18)
        self.txt.pack(fill=tk.BOTH, expand=True)
        self._append_log(f"[{APP_TITLE}] 準備完了。\n")

    def _append_log(self, line: str):
        self.txt.insert(tk.END, line + "\n")
        self.txt.see(tk.END)

    def _build_cmd(self):
        # 1) 凍結exeとして動いているかを判定
        is_frozen = bool(getattr(sys, "frozen", False))
        base_dir = os.path.dirname(sys.argv[0]) if is_frozen else os.path.dirname(__file__)
        cli = os.path.join(base_dir, "fanza_cli.exe")

        if os.path.exists(cli):
            cmd = [cli]
        else:
            if is_frozen:
                messagebox.showerror("実行に必要なファイルがありません", "fanza_cli.exe が見つかりません。同じフォルダに fanza_cli.exe を置いてください。")
                return None
            else:
                cmd = [sys.executable or "python", "-m", "app.main"]

        PH_KW = "例: 単体女優"
        PH_CID = "例: SSIS-123"
        PH_DATE = "YYYY-MM-DD"

        cmd += [
            "--hits", str(self.var_hits.get()),
            "--max", str(self.var_max.get()),
            "--sort", self.var_sort.get(),
            "--outfile", self.var_outfile.get()
        ]

        if self.var_site.get().strip():
            cmd += ["--site", self.var_site.get().strip()]
        if self.var_service.get().strip():
            cmd += ["--service", self.var_service.get().strip()]
        if self.var_floor.get().strip():
            cmd += ["--floor", self.var_floor.get().strip()]
        kw = self.var_keyword.get().strip()
        if kw and kw != PH_KW:
            cmd += ["--keyword", kw]
        c = self.var_cid.get().strip()
        if c and c != PH_CID:
            cmd += ["--cid", c]
        gte = self.var_gte.get().strip()
        if gte and gte != PH_DATE:
            cmd += ["--gte-date", gte]
        lte = self.var_lte.get().strip()
        if lte and lte != PH_DATE:
            cmd += ["--lte-date", lte]

        if self.var_verify.get():
            cmd.append("--verify-images")
        if self.var_no_content.get():
            cmd.append("--no-content")
        if self.var_skip_placeholder.get():
            cmd.append("--skip-placeholder")
        cmd += ["--min-samples", str(self.var_min_samples.get())]

        def add_regex(flag, s):
            if s.strip():
                cmd.extend([flag, s.strip()])
        add_regex("--include-maker", self.var_inc_maker.get())
        add_regex("--exclude-maker", self.var_exc_maker.get())
        add_regex("--include-actress", self.var_inc_actress.get())
        add_regex("--exclude-actress", self.var_exc_actress.get())
        add_regex("--include-genre", self.var_inc_genre.get())
        add_regex("--exclude-genre", self.var_exc_genre.get())
        add_regex("--include-title", self.var_inc_title.get())
        add_regex("--exclude-title", self.var_exc_title.get())
        add_regex("--include-cid-prefix", self.var_inc_cidp.get())
        add_regex("--exclude-cid-prefix", self.var_exc_cidp.get())

        if self.var_no_head.get():
            cmd.append("--no-head-check")
        cmd += ["--head-timeout", str(self.var_head_timeout.get())]
        if self.var_head_insecure.get():
            cmd.append("--head-insecure")

        # ★ 本文テンプレ系（CLIへ反映）
        if self.var_ctmpl.get().strip():
            cmd += ["--content-template", self.var_ctmpl.get().strip()]
        if self.var_cmdtmpl.get().strip():
            cmd += ["--content-md-template", self.var_cmdtmpl.get().strip()]
        if self.var_pre.get().strip():
            cmd += ["--prepend-html", self.var_pre.get().strip()]
        if self.var_post.get().strip():
            cmd += ["--append-html", self.var_post.get().strip()]
        if self.var_hook.get().strip():
            cmd += ["--content-hook", self.var_hook.get().strip()]
        cmd += ["--max-gallery", str(self.var_maxgal.get())]

        if self.var_wp_post.get():
            cmd.append("--wp-post")
            if self.var_future.get().strip():
                cmd += ["--future-datetime", self.var_future.get().strip()]
            if self.var_publish.get():
                cmd.append("--publish")
            if self.var_wp_cats.get().strip():
                cmd += ["--wp-categories", self.var_wp_cats.get().strip()]
            if self.var_wp_tags.get().strip():
                cmd += ["--wp-tags", self.var_wp_tags.get().strip()]

        return cmd

    def _on_run(self):
        # 検証: WP投稿を選んだのに認証未入力
        if self.var_wp_post.get():
            if not (self.var_wp_url.get().strip() and self.var_wp_user.get().strip() and self.var_wp_app.get().strip()):
                messagebox.showerror("設定不足", "WP投稿を行うには WP_URL / WP_USER / WP_APP_PASS が必要です。")
                return

        # 出力ディレクトリ
        try:
            outdir = os.path.dirname(self.var_outfile.get())
            if outdir and not os.path.exists(outdir):
                os.makedirs(outdir, exist_ok=True)
        except Exception as e:
            messagebox.showerror("パスエラー", f"出力先を作成できません: {e}")
            return

        cmd = self._build_cmd()
        if not cmd:
            return

        env = os.environ.copy()
        # APIキー/アフィリエイトID：引数でも渡して良いが既存mainは環境変数優先なのでここで注入
        if self.var_api_id.get().strip():
            env["API_ID"] = self.var_api_id.get().strip()
        if self.var_affid.get().strip():
            env["AFFILIATE_ID"] = self.var_affid.get().strip()
        # WP
        if self.var_wp_url.get().strip():
            env["WP_URL"] = self.var_wp_url.get().strip()
        if self.var_wp_user.get().strip():
            env["WP_USER"] = self.var_wp_user.get().strip()
        if self.var_wp_app.get().strip():
            env["WP_APP_PASS"] = self.var_wp_app.get().strip()
        if self.var_wp_cats.get().strip():
            env["WP_CATEGORIES"] = self.var_wp_cats.get().strip()
        if self.var_wp_tags.get().strip():
            env["WP_TAGS"] = self.var_wp_tags.get().strip()

        self._append_log("$ " + " ".join(shlex.quote(x) for x in cmd))
        self.btn_run.config(state=tk.DISABLED)
        try:
            self.runner.run(cmd, cwd=os.getcwd(), env=env)
        except Exception as e:
            messagebox.showerror("起動エラー", str(e))
            self.btn_run.config(state=tk.NORMAL)

    def _on_kill(self):
        self.runner.kill()
        self._append_log("[INFO] 強制終了シグナルを送信しました。")

    def _clear_log(self):
        self.txt.delete("1.0", tk.END)

    # -------------- Callbacks --------------
    def _on_proc_line(self, line: str):
        self._append_log(line)

    def _on_proc_exit(self, code: int | None):
        self._append_log(f"[EXIT] returncode={code}")
        self.btn_run.config(state=tk.NORMAL)

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
