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

# プロファイル保存（WP/APIキー一式）
try:
    from profile_store import ProfileStore, DesktopProfile
except Exception:  # パッケージ構成が変わっても GUI が落ちないように
    ProfileStore = None  # type: ignore
    DesktopProfile = None  # type: ignore

APP_TITLE = "FANZA Scraper Desktop"

# ------------------ Service/Floor Presets ------------------
# GUI で site/service/floor を“まとめて選ぶ”ためのプリセット。
# 追加したいときは、このリストに 1 行足すだけでOK（UIは自動で増える）。
#
# 形式: ("表示名", {"site": "...", "service": "...", "floor": "..."})
SERVICE_FLOOR_PRESETS = [
    ("動画（FANZA / digital / videoa）", {"site": "FANZA", "service": "digital", "floor": "videoa"}),
    ("素人（FANZA / digital / videoc）", {"site": "FANZA", "service": "digital", "floor": "videoc"}),
    ("同人（FANZA / doujin / digital_doujin）", {"site": "FANZA", "service": "doujin", "floor": "digital_doujin"}),
    ("漫画（FANZA / ebook / comic）", {"site": "FANZA", "service": "ebook", "floor": "comic"})
    # 例: 追加したい場合
    # ("エロマンガ（FANZA / ebook / comic）", {"site": "FANZA", "service": "ebook", "floor": "comic"}),
]

# ------------------ Helpers ------------------
def add_labeled_entry(parent, label, textvar, width=0, show=None, placeholder=None, state=None):
    row = ttk.Frame(parent)
    row.pack(fill=tk.X, padx=8, pady=4)
    ttk.Label(row, text=label, width=20, anchor=tk.W).pack(side=tk.LEFT)
    ent = ttk.Entry(row, textvariable=textvar, show=show)
    if state:
        ent.config(state=state)
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

class ProfileManagerDialog(tk.Toplevel):
    """WP/APIキー一式をプロファイルとして保存・編集するダイアログ。"""

    def __init__(self, master: tk.Misc, store: "ProfileStore"):
        super().__init__(master)
        self.title("プロファイル管理")
        self.geometry("780x420")
        self.resizable(True, True)
        self.store = store

        # Vars
        self.var_name = tk.StringVar()
        self.var_wp_url = tk.StringVar()
        self.var_wp_user = tk.StringVar()
        self.var_wp_app = tk.StringVar()
        self.var_api_id = tk.StringVar()
        self.var_aff_api = tk.StringVar()
        self.var_aff_link = tk.StringVar()

        self._build_ui()
        self._reload_list()

        # モーダルっぽくする
        self.transient(master)
        self.grab_set()

    def _build_ui(self):
        root = ttk.Frame(self)
        root.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        cols = ttk.Frame(root)
        cols.pack(fill=tk.BOTH, expand=True)

        # 左：一覧
        left = ttk.Frame(cols)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        ttk.Label(left, text="一覧").pack(anchor=tk.W)
        self.lst = tk.Listbox(left, height=14)
        self.lst.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        self.lst.bind("<<ListboxSelect>>", lambda _e: self._on_select())

        # 右：編集
        right = ttk.Frame(cols)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(12, 0))
        ttk.Label(right, text="編集（保存時のみ入力。普段は一覧から選ぶだけ）").pack(anchor=tk.W)

        form = ttk.LabelFrame(right, text="プロファイル")
        form.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        add_labeled_entry(form, "NAME", self.var_name)
        add_labeled_entry(form, "WP_URL", self.var_wp_url)
        add_labeled_entry(form, "WP_USER", self.var_wp_user)
        add_labeled_entry(form, "WP_APP_PASS", self.var_wp_app, show="*")
        add_labeled_entry(form, "API_ID", self.var_api_id)
        add_labeled_entry(form, "FANZA_API_AFFILIATE_ID", self.var_aff_api)
        add_labeled_entry(form, "FANZA_LINK_AFFILIATE_ID", self.var_aff_link)

        btns = ttk.Frame(right)
        btns.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(btns, text="新規", command=self._new).pack(side=tk.LEFT)
        ttk.Button(btns, text="保存/更新", command=self._save).pack(side=tk.LEFT, padx=8)
        ttk.Button(btns, text="削除", command=self._delete).pack(side=tk.LEFT)
        ttk.Button(btns, text="閉じる", command=self.destroy).pack(side=tk.RIGHT)

        ttk.Label(
            right,
            text="※ 平文保存です（共有PC/バックアップ/同期サービスに注意）",
        ).pack(anchor=tk.W, pady=(8, 0))

    def _reload_list(self, select: str | None = None):
        self.lst.delete(0, tk.END)
        names = self.store.list_names()
        for n in names:
            self.lst.insert(tk.END, n)

        if select and select in names:
            idx = names.index(select)
            self.lst.selection_set(idx)
            self.lst.see(idx)
            self._load_to_form(select)

    def _on_select(self):
        sel = self.lst.curselection()
        if not sel:
            return
        name = self.lst.get(sel[0])
        self._load_to_form(name)

    def _load_to_form(self, name: str):
        p = self.store.get(name)
        if not p:
            return
        self.var_name.set(p.name)
        self.var_wp_url.set(p.WP_URL)
        self.var_wp_user.set(p.WP_USER)
        self.var_wp_app.set(p.WP_APP_PASS)
        self.var_api_id.set(p.API_ID)
        self.var_aff_api.set(p.FANZA_API_AFFILIATE_ID)
        self.var_aff_link.set(p.FANZA_LINK_AFFILIATE_ID)

    def _new(self):
        self.var_name.set("")
        self.var_wp_url.set("")
        self.var_wp_user.set("")
        self.var_wp_app.set("")
        self.var_api_id.set("")
        self.var_aff_api.set("")
        self.var_aff_link.set("")
        self.lst.selection_clear(0, tk.END)

    def _save(self):
        name = (self.var_name.get() or "").strip()
        if not name:
            messagebox.showerror("NAME が必要", "プロファイル名（NAME）を入力してください")
            return

        prof = DesktopProfile(
            name=name,
            WP_URL=(self.var_wp_url.get() or "").strip(),
            WP_USER=(self.var_wp_user.get() or "").strip(),
            WP_APP_PASS=(self.var_wp_app.get() or "").strip(),
            API_ID=(self.var_api_id.get() or "").strip(),
            FANZA_API_AFFILIATE_ID=(self.var_aff_api.get() or "").strip(),
            FANZA_LINK_AFFILIATE_ID=(self.var_aff_link.get() or "").strip(),
        )

        self.store.upsert(prof)
        self.store.set_last_selected(name)
        try:
            self.store.save()
        except Exception as e:
            messagebox.showerror("保存失敗", str(e))
            return

        self._reload_list(select=name)
        messagebox.showinfo("保存完了", f"'{name}' を保存しました")

    def _delete(self):
        sel = self.lst.curselection()
        if not sel:
            messagebox.showerror("削除できません", "削除するプロファイルを一覧から選んでください")
            return
        name = self.lst.get(sel[0])
        if not messagebox.askyesno("確認", f"'{name}' を削除しますか？"):
            return
        self.store.delete(name)
        try:
            self.store.save()
        except Exception as e:
            messagebox.showerror("保存失敗", str(e))
            return
        self._new()
        self._reload_list()

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

        # ★ 先に属性だけ必ず作る（init途中で例外が起きても AttributeError を防ぐ）
        self.profile_store = None

        self._load_env()

        # ★ import 成功してる時だけロード
        if ProfileStore:
            self.profile_store = ProfileStore()
            self.profile_store.load()

        self.runner = ProcessRunner(self._on_proc_line, self._on_proc_exit)
        self._build_ui()

        # プリセット初期表示（現在の site/service/floor に一致するものがあれば選択）
        self._sync_preset_from_vars()

        # ★ 起動時にプロファイル反映
        if self.profile_store and DesktopProfile:
            self._bootstrap_profiles_from_env()
            self._refresh_profile_list()
            self._select_initial_profile()

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

    # -------------- Presets --------------
    def _on_preset_selected(self):
        """
        プリセットを選ぶと site/service/floor をまとめて反映する。
        追加は SERVICE_FLOOR_PRESETS に 1 行足すだけ。
        """
        name = (self.var_preset.get() or "").strip()
        if not name:
            return
        mapping = None
        for n, v in SERVICE_FLOOR_PRESETS:
            if n == name:
                mapping = v
                break
        if not mapping:
            return
        self.var_site.set(mapping.get("site", ""))
        self.var_service.set(mapping.get("service", ""))
        self.var_floor.set(mapping.get("floor", ""))

    def _sync_preset_from_vars(self):
        """現在の site/service/floor と一致するプリセットがあれば選択状態にする（任意）。"""
        cur = (self.var_site.get().strip(), self.var_service.get().strip(), self.var_floor.get().strip())
        for name, v in SERVICE_FLOOR_PRESETS:
            if (v.get("site", "").strip(), v.get("service", "").strip(), v.get("floor", "").strip()) == cur:
                try:
                    self.var_preset.set(name)
                except Exception:
                    pass
                return


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
        self.var_preset = tk.StringVar(value="")
        self.var_keyword = tk.StringVar()
        self.var_cid = tk.StringVar()
        self.var_gte = tk.StringVar()
        self.var_lte = tk.StringVar()

        # NEW: 自動スキップ（出力フォルダの既存CSVからCID収集）
        self.var_auto_skip_outputs = tk.BooleanVar(value=True)
        self.var_ledger        = tk.StringVar()

        # NEW: 新規N件＋既存は更新しない
        self.var_target_new = tk.IntVar(value=0)
        self.var_no_update_existing = tk.BooleanVar(value=False)

        topA = ttk.LabelFrame(tab_basic, text="取得条件")
        topA.pack(fill=tk.X, padx=6, pady=6)

        # プリセット（site/service/floor をまとめて切り替え）
        row_preset = ttk.Frame(topA)
        row_preset.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(row_preset, text="プリセット", width=20, anchor=tk.W).pack(side=tk.LEFT)
        self.cmb_preset = ttk.Combobox(
            row_preset,
            textvariable=self.var_preset,
            state="readonly",
            values=[name for name, _v in SERVICE_FLOOR_PRESETS],
        )
        self.cmb_preset.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.cmb_preset.bind("<<ComboboxSelected>>", lambda _e: self._on_preset_selected())

        # site/service/floor（手入力もできるように Entry のまま）
        row_ssf = ttk.Frame(topA)
        row_ssf.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(row_ssf, text="site", width=20, anchor=tk.W).pack(side=tk.LEFT)
        ttk.Entry(row_ssf, textvariable=self.var_site).pack(side=tk.LEFT, fill=tk.X, expand=True)

        row_ssf2 = ttk.Frame(topA)
        row_ssf2.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(row_ssf2, text="service", width=20, anchor=tk.W).pack(side=tk.LEFT)
        ttk.Entry(row_ssf2, textvariable=self.var_service).pack(side=tk.LEFT, fill=tk.X, expand=True)

        row_ssf3 = ttk.Frame(topA)
        row_ssf3.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(row_ssf3, text="floor", width=20, anchor=tk.W).pack(side=tk.LEFT)
        ttk.Entry(row_ssf3, textvariable=self.var_floor).pack(side=tk.LEFT, fill=tk.X, expand=True)

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

        # NEW: 重複スキップ / 台帳
        topD = ttk.LabelFrame(tab_basic, text="重複スキップ / 台帳")
        topD.pack(fill=tk.X, padx=6, pady=6)
        # auto-skip-outputs
        rowd1 = ttk.Frame(topD); rowd1.pack(fill=tk.X, pady=4)
        ttk.Checkbutton(rowd1, text="すでに出力済みCSVから自動スキップ (--auto-skip-outputs)",
                        variable=self.var_auto_skip_outputs).pack(side=tk.LEFT)
        # ledger
        rowd2 = ttk.Frame(topD); rowd2.pack(fill=tk.X, pady=4)
        ttk.Label(rowd2, text="Ledger CSV (--ledger)").pack(side=tk.LEFT)
        ent_ledger = ttk.Entry(rowd2, textvariable=self.var_ledger)
        ent_ledger.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 6))
        ttk.Button(rowd2, text="参照…", command=self._browse_ledger).pack(side=tk.LEFT)

        # NEW: 目標新規数＆既存更新オプション
        row3 = ttk.Frame(topB); row3.pack(fill=tk.X, pady=4)
        ttk.Label(row3, text="新規が N 件たまるまで (--target-new)").pack(side=tk.LEFT)
        ttk.Spinbox(row3, from_=0, to=1000, textvariable=self.var_target_new, width=8).pack(side=tk.LEFT, padx=(6, 12))
        ttk.Checkbutton(row3, text="既存は更新しない (--no-update-existing)",
                        variable=self.var_no_update_existing).pack(side=tk.LEFT)

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
        # プロファイル選択（WP/APIキー一式）
        self.var_profile = tk.StringVar(value="")
        prof = ttk.LabelFrame(tab_wp, text="接続プロファイル（WP/APIキー一式をまとめて保存）")
        prof.pack(fill=tk.X, padx=6, pady=6)
        rowp = ttk.Frame(prof)
        rowp.pack(fill=tk.X, padx=8, pady=6)
        ttk.Label(rowp, text="プロファイル").pack(side=tk.LEFT)
        self.cmb_profile = ttk.Combobox(rowp, textvariable=self.var_profile, state="readonly", values=[])
        self.cmb_profile.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 6))
        self.cmb_profile.bind("<<ComboboxSelected>>", lambda _e: self._on_profile_selected())
        ttk.Button(rowp, text="管理...", command=self._open_profile_manager).pack(side=tk.LEFT)
        ttk.Label(
            prof,
            text="※ profiles.json に平文で保存されます。共有PCでは注意。",
        ).pack(anchor=tk.W, padx=8, pady=(0, 6))

        self.var_wp_post = tk.BooleanVar(value=False)
        self.var_wp_url = tk.StringVar(value=os.getenv("WP_URL", ""))
        self.var_wp_user = tk.StringVar(value=os.getenv("WP_USER", ""))
        self.var_wp_app = tk.StringVar(value=os.getenv("WP_APP_PASS", ""))
        self.var_wp_cats = tk.StringVar(value=os.getenv("WP_CATEGORIES", ""))
        self.var_wp_tags = tk.StringVar(value=os.getenv("WP_TAGS", ""))
        self.var_publish = tk.BooleanVar(value=False)
        self.var_mirror  = tk.BooleanVar(value=False)  # ★ 画像ミラー（WPメディアへアップロード）
        self.var_future = tk.StringVar()

        wpf = ttk.LabelFrame(tab_wp, text="WordPress 投稿設定")
        wpf.pack(fill=tk.X, padx=6, pady=6)
        ttk.Checkbutton(wpf, text="RESTで投稿する (--wp-post)", variable=self.var_wp_post).pack(fill=tk.X, padx=8, pady=4)

        # プロファイル経由で入力するため、基本は編集不可（管理画面で編集）
        add_labeled_entry(wpf, "WP_URL", self.var_wp_url, state="disabled")
        add_labeled_entry(wpf, "WP_USER", self.var_wp_user, state="disabled")
        add_labeled_entry(wpf, "WP_APP_PASS", self.var_wp_app, show="*", state="disabled")

        # ★ 画像をWPメディアへミラーして本文URLをローカル化
        ttk.Checkbutton(
            wpf, text="画像をWPにミラーして本文URLをローカル化 (--mirror-images)", variable=self.var_mirror
        ).pack(fill=tk.X, padx=8, pady=4)
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
        # 作品情報取得用 affiliate_id（APIに渡す用）
        self.var_affid_api = tk.StringVar(
            value=os.getenv("FANZA_API_AFFILIATE_ID", os.getenv("AFFILIATE_ID", ""))
        )
        # 実際のリンク用 affiliate_id（WordPress に貼るアフィリンク用）
        self.var_affid_link = tk.StringVar(
            value=os.getenv("FANZA_LINK_AFFILIATE_ID", os.getenv("AFFILIATE_ID", ""))
        )
        envf = ttk.LabelFrame(
            tab_advanced,
            text="APIキー / アフィリエイトID（環境変数として注入）",
        )
        envf.pack(fill=tk.X, padx=6, pady=6)

        # プロファイル経由で入力するため、基本は編集不可（管理画面で編集）
        add_labeled_entry(envf, "API_ID", self.var_api_id, state="disabled")
        add_labeled_entry(envf, "FANZA_API_AFFILIATE_ID（取得用）", self.var_affid_api, state="disabled")
        add_labeled_entry(envf, "FANZA_LINK_AFFILIATE_ID（リンク用）", self.var_affid_link, state="disabled")

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

    # -------------- Profiles --------------
    def _bootstrap_profiles_from_env(self):
        """profiles.json が空のとき、.env の値で 'env' プロファイルを作る（静かに）。"""
        store = getattr(self, "profile_store", None)
        if not store or not DesktopProfile:
            return

        if store.list_names():
            return

        env_profile = DesktopProfile(
            name="env",
            WP_URL=os.getenv("WP_URL", ""),
            WP_USER=os.getenv("WP_USER", ""),
            WP_APP_PASS=os.getenv("WP_APP_PASS", ""),
            API_ID=os.getenv("API_ID", ""),
            FANZA_API_AFFILIATE_ID=os.getenv("FANZA_API_AFFILIATE_ID", os.getenv("AFFILIATE_ID", "")),
            FANZA_LINK_AFFILIATE_ID=os.getenv("FANZA_LINK_AFFILIATE_ID", os.getenv("AFFILIATE_ID", "")),
        )

        # 何も入っていない env は作らない
        if any([
            env_profile.WP_URL,
            env_profile.WP_USER,
            env_profile.WP_APP_PASS,
            env_profile.API_ID,
            env_profile.FANZA_API_AFFILIATE_ID,
            env_profile.FANZA_LINK_AFFILIATE_ID,
        ]):
            store.upsert(env_profile)
            store.set_last_selected("env")
            store.save()

    def _refresh_profile_list(self):
        if not self.profile_store:
            return
        names = self.profile_store.list_names()
        try:
            self.cmb_profile["values"] = names
        except Exception:
            pass

    def _select_initial_profile(self):
        if not self.profile_store:
            return

        names = self.profile_store.list_names()
        if not names:
            # 未作成なら空のまま（管理画面で作成してもらう）
            return

        last = self.profile_store.last_selected()
        if last and last in names:
            self.var_profile.set(last)
            self._apply_profile(last)
            return

        # 先頭を選ぶ
        self.var_profile.set(names[0])
        self._apply_profile(names[0])

    def _on_profile_selected(self):
        name = (self.var_profile.get() or "").strip()
        if not name:
            return
        self._apply_profile(name)

    def _apply_profile(self, name: str):
        if not self.profile_store:
            return

        prof = self.profile_store.get(name)
        if not prof:
            return

        # 画面へ反映（state=disabledでも StringVar は更新可能）
        self.var_wp_url.set(prof.WP_URL)
        self.var_wp_user.set(prof.WP_USER)
        self.var_wp_app.set(prof.WP_APP_PASS)
        self.var_api_id.set(prof.API_ID)
        self.var_affid_api.set(prof.FANZA_API_AFFILIATE_ID)
        self.var_affid_link.set(prof.FANZA_LINK_AFFILIATE_ID)

        # 記録
        self.profile_store.set_last_selected(name)
        self.profile_store.save()

    def _open_profile_manager(self):
        # ★ ここが重要：属性が無くても落ちないようにする
        store = getattr(self, "profile_store", None)

        if (store is None) or (DesktopProfile is None):
            messagebox.showerror(
                "利用できません",
                "プロファイル機能が初期化されていません。\n"
                "・profile_store.py / app.util.profile_store の import に失敗している\n"
                "・App.__init__ で self.profile_store を作る前に例外で落ちている\n"
                "のどちらかです。",
            )
            return

        dlg = ProfileManagerDialog(self, store)
        self.wait_window(dlg)

        # 反映
        self._refresh_profile_list()
        self._select_initial_profile()


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

        # NEW: 新規N件・既存は更新しない
        if self.var_target_new.get() > 0:
            cmd += ["--target-new", str(self.var_target_new.get())]
        if self.var_no_update_existing.get():
            cmd += ["--no-update-existing"]

        # NEW: 自動スキップ / 台帳
        if self.var_auto_skip_outputs.get():
            cmd += ["--auto-skip-outputs"]
        if self.var_ledger.get().strip():
            cmd += ["--ledger", self.var_ledger.get().strip()]

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

        # ★ 画像ミラー（WP投稿の有無に関わらず有効にできる）
        if self.var_mirror.get(): cmd.append("--mirror-images")

        return cmd

    def _on_run(self):
        # 検証: WP投稿 or 画像ミラー時は認証が必須
        if self.var_wp_post.get() or self.var_mirror.get():
            if not (self.var_wp_url.get().strip() and self.var_wp_user.get().strip() and self.var_wp_app.get().strip()):
                messagebox.showerror(
                    "設定不足",
                    "WP投稿/画像ミラーを有効にしていますが、WordPress 認証が空です。\n"
                    "『WordPress』タブのプロファイルを選択するか、『管理...』で登録してください。",
                )
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

        api_aff = self.var_affid_api.get().strip()
        link_aff = self.var_affid_link.get().strip()

        if api_aff:
            # 作品情報取得用 affiliate_id（APIに渡す用）
            env["FANZA_API_AFFILIATE_ID"] = api_aff
        if link_aff:
            # リンク用 affiliate_id。後方互換のため AFFILIATE_ID にも入れておく
            env["FANZA_LINK_AFFILIATE_ID"] = link_aff
            env["AFFILIATE_ID"] = link_aff
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

    def _browse_ledger(self):
        path = filedialog.asksaveasfilename(
            title="Ledger CSV を保存",
            defaultextension=".csv",
            filetypes=[("CSV", ".csv"), ("All Files", "*.*")]
        )
        if path:
            self.var_ledger.set(path)

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
