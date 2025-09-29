import os
import re
import csv
import json
import time
import yaml
import base64
import sqlite3
import shutil
import subprocess
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from tqdm import tqdm
from yt_dlp import YoutubeDL

# Selenium Wire
from seleniumwire import webdriver  # type: ignore
from selenium.webdriver.chrome.options import Options  # type: ignore


@dataclass
class VodItem:
    title: str
    year: str
    region: str
    category: str
    detail_url: str
    cover: str
    play_pages: list
    score: str | None = None
    brief: str | None = None


class Storage:
    def __init__(self, json_path: str, csv_path: str, sqlite_path: str):
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        self.json_fp = open(json_path, "a", encoding="utf-8")
        self.csv_fp = open(csv_path, "a", encoding="utf-8", newline="")
        self.csv_writer = None
        self.sqlite_path = sqlite_path
        self._init_sqlite()

    def _init_sqlite(self):
        conn = sqlite3.connect(self.sqlite_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS simpsons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                year TEXT,
                region TEXT,
                category TEXT,
                detail_url TEXT,
                cover TEXT,
                score TEXT,
                brief TEXT,
                play_pages TEXT,
                player_json TEXT,
                m3u8_url TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    def write_item(self, item: VodItem, player_json: dict | None, m3u8_url: str | None):
        data = asdict(item)
        data["play_pages"] = item.play_pages
        line = json.dumps({**data, "player_aaaa": player_json, "m3u8": m3u8_url}, ensure_ascii=False)
        self.json_fp.write(line + "\n")
        if self.csv_writer is None:
            self.csv_writer = csv.DictWriter(self.csv_fp, fieldnames=list(data.keys()) + ["player_aaaa", "m3u8"])
            self.csv_writer.writeheader()
        row = {**data, "player_aaaa": json.dumps(player_json, ensure_ascii=False) if player_json else "", "m3u8": m3u8_url or ""}
        self.csv_writer.writerow(row)
        conn = sqlite3.connect(self.sqlite_path)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO simpsons(title, year, region, category, detail_url, cover, score, brief, play_pages, player_json, m3u8_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.title,
                item.year,
                item.region,
                item.category,
                item.detail_url,
                item.cover,
                item.score,
                item.brief,
                json.dumps(item.play_pages, ensure_ascii=False),
                json.dumps(player_json, ensure_ascii=False) if player_json else None,
                m3u8_url,
            ),
        )
        conn.commit()
        conn.close()

    def close(self):
        try:
            self.json_fp.close()
        except Exception:
            pass
        try:
            self.csv_fp.close()
        except Exception:
            pass


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_chrome(headless: bool, user_agent: str | None) -> webdriver.Chrome:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    if user_agent and user_agent != "auto":
        chrome_options.add_argument(f"--user-agent={user_agent}")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def parse_detail_with_driver(driver: webdriver.Chrome, detail_url: str) -> VodItem | None:
    driver.get(detail_url)
    time.sleep(3)
    html = driver.page_source
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.select_one(".myui-panel__head h1, h1.title, .myui-panel__head .title, h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title:
        title = soup.title.get_text(strip=True) if soup.title else ""
    cover_el = soup.select_one(".myui-vodlist__thumb, .myui-content__thumb img, .lazyload, .cover img")
    cover = cover_el.get("data-original") or cover_el.get("src") if cover_el else ""
    txt = soup.get_text("/", strip=True)
    m_year = re.search(r"(\d{4})(?:/|年)", txt)
    year = m_year.group(1) if m_year else ""
    region = "美国" if "美国" in txt else ""
    category = "美剧" if "美剧" in txt else ""
    play_links = []
    for a in soup.select("#playlist a[href*='/play/']"):
        href = a.get("href") or ""
        if href:
            play_links.append(urljoin(detail_url, href))
    if not play_links:
        found = set()
        for a in soup.find_all("a", href=True):
            if "/play/" in a["href"]:
                found.add(urljoin(detail_url, a["href"]))
        play_links = sorted(found)
    if not title or not play_links:
        return None
    return VodItem(
        title=title,
        year=year,
        region=region,
        category=category,
        detail_url=detail_url,
        cover=cover or "",
        play_pages=play_links,
        score=None,
        brief=None,
    )


def intercept_m3u8(driver: webdriver.Chrome, play_url: str) -> tuple[dict | None, str | None]:
    # 扩大拦截范围，包括更多视频格式
    driver.scopes = [r".*\.(m3u8|mp4|ts|flv|avi|mkv).*"]
    try:
        del driver.requests
    except Exception:
        pass
    
    driver.get(play_url)
    time.sleep(8)  # 增加等待时间
    
    player_json = None
    m3u8_url = None
    
    # 尝试解析player_aaaa
    try:
        page_html = driver.page_source
        m = re.search(r"var\s+player_aaaa\s*=\s*(\{[\s\S]*?\})\s*<", page_html)
        if m:
            j = m.group(1)
            j = j.replace("\n", " ")
            j = re.sub(r"(\w+):", r'"\1":', j)
            j = j.replace("'", '"')
            j = re.sub(r",\s*}\s*$", "}", j)
            try:
                player_json = json.loads(j)
                # 尝试从player_aaaa中提取URL
                if player_json and "url" in player_json:
                    url = player_json["url"]
                    if url and not url.startswith("http"):
                        # 如果是相对路径，尝试构建完整URL
                        if url.startswith("/"):
                            m3u8_url = f"https://www.mjw7.cc{url}"
                        else:
                            m3u8_url = f"https://www.mjw7.cc/{url}"
                    elif url and url.startswith("http"):
                        m3u8_url = url
            except Exception:
                player_json = None
    except Exception:
        player_json = None
    
    # 检查网络请求中的m3u8链接
    for req in driver.requests:
        if req.response and req.url:
            if ".m3u8" in req.url:
                m3u8_url = req.url
                break
            # 也检查其他视频格式
            elif any(ext in req.url for ext in [".mp4", ".ts", ".flv"]):
                m3u8_url = req.url
                break
    
    # 如果还没找到，再等待一下
    if not m3u8_url:
        time.sleep(5)
        for req in driver.requests:
            if req.response and req.url:
                if ".m3u8" in req.url:
                    m3u8_url = req.url
                    break
                elif any(ext in req.url for ext in [".mp4", ".ts", ".flv"]):
                    m3u8_url = req.url
                    break
    
    return player_json, m3u8_url


def download_with_ytdlp(m3u8_url: str, save_path: str):
    print(f"开始下载: {m3u8_url}")
    print(f"保存路径: {save_path}")
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    ffmpeg_exists = shutil.which("ffmpeg") is not None

    # 若无ffmpeg，改为.ts并使用原生HLS，避免伪MP4
    final_path = save_path
    base, ext = os.path.splitext(save_path)
    ydl_opts = {
        "outtmpl": final_path,
        "retries": 5,
        "fragment_retries": 5,
        "nocheckcertificate": True,
        "quiet": False,
        "noprogress": False,
        "verbose": True,  # 添加详细日志
    }
    if ffmpeg_exists:
        # 使用ffmpeg重封装为标准MP4，优化音画同步
        if ext.lower() != ".mp4":
            final_path = base + ".mp4"
            ydl_opts["outtmpl"] = final_path
        ydl_opts["merge_output_format"] = "mp4"
        ydl_opts["postprocessors"] = [
            {
                "key": "FFmpegVideoRemuxer", 
                "preferedformat": "mp4"
            }
        ]
        # 使用ffmpeg_args参数来传递自定义ffmpeg参数
        ydl_opts["ffmpeg_args"] = [
            "-c:v", "copy",  # 视频流直接复制，避免重编码
            "-c:a", "aac",   # 音频重编码为AAC确保兼容性
            "-strict", "experimental",  # 允许实验性编码器
            "-avoid_negative_ts", "make_zero",  # 避免负时间戳
            "-fflags", "+genpts",  # 生成PTS时间戳
            "-async", "1",  # 音频同步
            "-vsync", "cfr",  # 恒定帧率
        ]
    else:
        # 无ffmpeg则下载为.ts，避免"没有注册类"问题
        final_path = base + ".ts"
        ydl_opts["outtmpl"] = final_path
        ydl_opts["hls_prefer_native"] = True

    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([m3u8_url])
        print(f"✓ 下载完成: {final_path}")
    except Exception as e:
        print(f"✗ 下载失败: {e}")
        raise


def fix_audio_sync(file_path: str) -> bool:
    """修复单个文件的音画同步问题"""
    if not os.path.exists(file_path):
        return False
    
    ffmpeg_exists = shutil.which("ffmpeg") is not None
    if not ffmpeg_exists:
        print(f"警告: 未找到ffmpeg，无法修复 {file_path}")
        return False
    
    base, ext = os.path.splitext(file_path)
    temp_path = base + "_fixed_temp" + ext
    final_path = base + "_fixed" + ext
    
    try:
        # 使用ffmpeg修复音画同步
        cmd = [
            "ffmpeg", "-i", file_path,
            "-c:v", "copy",  # 视频流直接复制
            "-c:a", "aac",   # 音频重编码为AAC
            "-strict", "experimental",
            "-avoid_negative_ts", "make_zero",
            "-fflags", "+genpts",
            "-async", "1",
            "-vsync", "cfr",
            "-y",  # 覆盖输出文件
            temp_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            # 重命名临时文件
            os.rename(temp_path, final_path)
            print(f"✓ 修复完成: {final_path}")
            return True
        else:
            print(f"✗ 修复失败: {file_path} - {result.stderr}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return False
            
    except Exception as e:
        print(f"✗ 修复异常: {file_path} - {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False


def batch_fix_downloads(download_dir: str):
    """批量修复下载目录中的视频文件音画同步问题"""
    if not os.path.exists(download_dir):
        print(f"下载目录不存在: {download_dir}")
        return
    
    video_extensions = ['.mp4', '.ts', '.mkv', '.avi', '.mov']
    video_files = []
    
    # 扫描所有视频文件
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            if any(file.lower().endswith(ext) for ext in video_extensions):
                video_files.append(os.path.join(root, file))
    
    if not video_files:
        print(f"在 {download_dir} 中未找到视频文件")
        return
    
    print(f"找到 {len(video_files)} 个视频文件，开始批量修复...")
    
    success_count = 0
    for video_file in tqdm(video_files, desc="修复音画同步"):
        if fix_audio_sync(video_file):
            success_count += 1
    
    print(f"修复完成: {success_count}/{len(video_files)} 个文件")


def main():
    cfg = load_config(os.path.join(os.path.dirname(__file__), "crawler_config.yaml"))
    
    # 检查是否需要批量修复历史下载
    if cfg.get("fix_audio_sync", {}).get("enabled", False):
        download_dir = cfg.get("fix_audio_sync", {}).get("download_dir", "downloads")
        print("开始批量修复历史下载文件的音画同步问题...")
        batch_fix_downloads(download_dir)
        return
    
    start_urls = cfg.get("start_urls", [])
    headless = True
    user_agent = cfg.get("headers", {}).get("user_agent")

    storage = Storage(
        json_path=cfg["output"]["json_path"],
        csv_path=cfg["output"]["csv_path"],
        sqlite_path=cfg["output"]["sqlite_path"],
    )

    driver = build_chrome(headless=headless, user_agent=None if user_agent == "auto" else user_agent)

    try:
        items: list[VodItem] = []
        for url in start_urls:
            item = parse_detail_with_driver(driver, url)
            if item:
                items.append(item)
            else:
                tqdm.write(f"详情解析失败: {url}")

        for item in tqdm(items, desc="详情&播放解析"):
            episode_index = 0
            for play_link in item.play_pages:
                episode_index += 1
                player_json, m3u8_url = intercept_m3u8(driver, play_link)
                storage.write_item(item, player_json, m3u8_url)
                if cfg.get("download", {}).get("enabled") and m3u8_url:
                    save_dir = cfg.get("download", {}).get("save_dir", "downloads")
                    filename_tpl = cfg.get("download", {}).get("filename_tpl", "{title}_E{episode}.mp4")
                    filename = filename_tpl.format(title=item.title, episode=f"{episode_index:02d}", id="")
                    save_path = os.path.join(save_dir, filename)
                    try:
                        download_with_ytdlp(m3u8_url, save_path)
                    except Exception as e:
                        tqdm.write(f"下载失败: {m3u8_url} -> {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        storage.close()


if __name__ == "__main__":
    main()
