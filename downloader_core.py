import os
import threading
import asyncio
import requests
from urllib.parse import urlparse
import urllib3

# Suppress only the single InsecureRequestWarning from urllib3 needed for verify=False.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import re
import mimetypes
import json
from urllib.parse import urlparse
import math
import shutil
import queue
import time
import sys
import subprocess
import shutil # --- جديد: استيراد مكتبة shutil للتحقق من وجود ffmpeg ---

# --- محاولة اسئتيراد مكتبة تحميل الفيديوهات ---
# --- إصلاح: فصل التحقق من كل مكتبة على حدة لتجنب الأخطاء المنطقية ---
try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    yt_dlp = None
    YTDLP_AVAILABLE = False

try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

# --- محاولة استيراد مكتبة cloudscraper (لحل روابط JS) ---
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    cloudscraper = None
    CLOUDSCRAPER_AVAILABLE = False

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    cloudscraper = None
    CLOUDSCRAPER_AVAILABLE = False

# --- Constants ---
STATE_FILE = "download_state.json"
NUM_SEGMENTS = 5
MAX_CONCURRENT_DOWNLOADS = 3

# --- Helper Functions ---
def format_size(size_bytes):
    if size_bytes is None or size_bytes <= 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB") # استخدام الوحدات المألوفة
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}" # إزالة "i" من اسم الوحدة

def format_eta(seconds):
    if seconds is None or seconds == float('inf') or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    if hours > 0:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    return f"{minutes:02}:{seconds:02}"

def find_unique_filepath(directory, filename):
    """
    Checks if a file exists at the given path. If it does, it appends a number
    (e.g., " (2)") to the filename until a unique path is found.
    """
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        return filepath

    base, ext = os.path.splitext(filename)
    counter = 2
    while True:
        new_filename = f"{base} ({counter}){ext}"
        new_filepath = os.path.join(directory, new_filename)
        if not os.path.exists(new_filepath):
            return new_filepath
        counter += 1

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def resolve_final_url(url, headers=None):
    """
    Resolves a URL from a hosting service (like MediaFire) to a direct download link.
    If the URL doesn't need special handling or if resolving fails, it returns the original URL.
    """
    if not url:
        return url # Return the same value (None or empty) to be handled by the caller.
 
    headers = headers or {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
 
    # --- MediaFire ---
    if "mediafire.com" in url:
        try:
            # --- Professional Fix: Use cloudscraper to bypass JS challenges ---
            # This is much more reliable than simple requests for sites like MediaFire.
            if CLOUDSCRAPER_AVAILABLE:
                scraper = cloudscraper.create_scraper()
                r = scraper.get(url, headers=headers, timeout=15)
            else:
                # Fallback to requests if cloudscraper is not installed
                r = requests.get(url, headers=headers, timeout=15, verify=False, allow_redirects=True)

            r.raise_for_status()
            html = r.text

            # Priority 1: Look for the official download button.
            m = re.search(r'href="(https://download[^"]+)"[^>]*>Download', html, re.IGNORECASE)
            if m:
                candidate = m.group(1)
                # Filter out intermediate php links.
                if not candidate.lower().endswith(".php"):
                    return candidate

            # Priority 2: If the button link was invalid or not found, look for any
            # direct download link that has a known, valid file extension.
            m = re.search(r'(https://download[^\s"\']+)', html)
            if m:
                return m.group(1)

            # Fallback: If no valid link is found, return the original URL.
            return url

        except Exception as e:
            print(f"MediaFire resolve error: {e}")
            return url
 
    # --- Google Drive ---
    if "drive.google.com" in url:
        m = re.search(r'/file/d/([^/]+)/', url)
        if m:
            file_id = m.group(1)
            return f"https://drive.google.com/uc?export=download&id={file_id}"
 
    # --- Dropbox ---
    if "dropbox.com" in url and "?dl=0" in url:
        return url.replace("?dl=0", "?dl=1")
 
    return url # Default: return the original URL

def _parse_details_from_response(response):
    """Helper to extract filename and size from a requests response object."""
    filename = ""
    disp = response.headers.get('content-disposition')
    if disp:
        filenames = re.findall('filename="?([^"]+)"?', disp)
        if filenames:
            filename = filenames[0]
 
    if not filename:
        parsed_url = urlparse(response.url) # Use the final URL after redirects
        filename = os.path.basename(parsed_url.path) or "downloaded_file"
 
    if not os.path.splitext(filename)[1]:
        content_type = response.headers.get('content-type')
        if content_type:
            ext = mimetypes.guess_extension(content_type.split(';')[0])
            if ext: filename += ext
    
    content_length = response.headers.get('content-length')
    total_size = int(content_length) if content_length is not None else None
    
    return filename, total_size
 
def get_download_details(url):
    """Probes a URL to get the filename and total size, trying HEAD first for speed."""
    # --- Safeguard: Check for invalid URL at the very beginning ---
    if not url:
        return None, None, "الرابط غير صالح أو فارغ."

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
        'Connection': 'keep-alive',
        'Referer': url, # إضافة Referer مهم جداً لتجنب أخطاء 403
    }
    
    # --- Step 1: Resolve the URL to a direct link if necessary ---
    try:
        url = resolve_final_url(url, headers)
    except Exception as e:
        return None, None, f"فشل في تحليل الرابط: {e}"

    if not url:
        return None, None, "تعذر الحصول على الرابط النهائي بعد محاولة التحليل."

    def _handle_exception(e, url_str):
        # --- تحسين رسائل الأخطاء لتكون أكثر وضوحاً للمستخدم ---
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
            return None, None, "تم حظر الطلب مؤقتاً (429). حاول مرة أخرى بعد فترة."
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
            return None, None, "فشل العثور على الملف (404). الرابط قد يكون منتهي الصلاحية أو محذوفاً."
        if isinstance(e, requests.exceptions.ConnectionError) and isinstance(e.args[0], urllib3.exceptions.MaxRetryError) and isinstance(e.args[0].reason, urllib3.exceptions.NewConnectionError):
            return None, None, "فشل الاتصال بالخادم. تحقق من اتصالك بالإنترنت أو أن الموقع يعمل."
        if isinstance(e, requests.exceptions.ConnectionError) and 'Connection aborted' in str(e):
            return None, None, "قطع الخادم الاتصال. قد يكون الموقع محمياً ضد التحميل الآلي. جرب رابطاً آخر."
        error_message = f"فشل الاتصال بالخادم: {e}"
        print(f"Request for details failed for URL '{url_str}': {e}")
        return None, None, error_message

    try:
        # Try a HEAD request first - it's much faster as it doesn't download the body.
        with requests.head(url, allow_redirects=True, timeout=15, headers=headers, verify=False) as r:
            r.raise_for_status()
            filename, total_size = _parse_details_from_response(r)
            # Some servers don't return Content-Length for HEAD requests. If so, fall back to GET.
            if total_size is not None:
                return filename, total_size, None
    except requests.exceptions.RequestException as e:
        # If HEAD fails with 429, fail fast. Otherwise, fall through to GET.
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
            return _handle_exception(e, url) # Pass the original URL here
        pass
 
    # Fallback to GET request if HEAD failed or didn't provide content-length
    try:
        with requests.get(url, stream=True, allow_redirects=True, timeout=30, headers=headers, verify=False) as r:
            r.raise_for_status()
            filename, total_size = _parse_details_from_response(r)
            return filename, total_size, None
    except requests.exceptions.RequestException as e:
        return _handle_exception(e, url)

class DownloadTask(threading.Thread):
    def __init__(self, task_id, url, filepath, total_size, update_queue=None, force_single_thread=False, http_headers=None, webpage_url=None, start_paused=False, repair_mode=False, update_callback=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.task_id = task_id
        self.url = url
        self.filepath = filepath
        self.total_size = total_size
        self.update_callback = update_callback
        self.update_queue = update_queue
        self.force_single_thread = force_single_thread or (total_size is None)
        
        # --- State Flags ---
        self.stop_flag = threading.Event()
        self.is_paused = start_paused
        self.error_flag = threading.Event()
        self.error_message = ""
        self.exit_on_pause = False # Flag to signal thread to exit after pausing and saving state
        self.repair_mode = repair_mode # New flag for smart repair

        # --- Internal ---
        self.state = load_state()
        self.smoothed_speed = 0.0
        self.SMOOTHING_FACTOR = 0.2
        self.segment_progress = [0] * NUM_SEGMENTS
        self.worker_threads = []
        # If specific headers are provided (e.g., from yt-dlp), they will be used.
        # Otherwise, we define a default set of browser-like headers to improve
        # compatibility and avoid '403 Forbidden' errors from servers that
        # block simple script user-agents.
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*', 'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive', 'Referer': webpage_url or url,
        }
        self.http_headers = http_headers or default_headers
        self.webpage_url = webpage_url

    def _send_update(self, data):
        """Helper to send updates via callback or queue."""
        if self.update_callback:
            self.update_callback(data)
        elif self.update_queue:
            self.update_queue.put(data)

    def toggle_pause_resume(self):
        """Toggles the pause/resume state of the download."""
        self.is_paused = not self.is_paused
        if self.is_paused:
            # The run() loop will detect this change, save state, and send the 'paused' update.
            pass
        else:
            # Resuming: send a signal to the UI to update its state.
            self._send_update({"task_id": self.task_id, "type": "resumed"})

    def pause_and_exit(self):
        """A special method to pause the download and signal the thread to exit cleanly."""
        self.exit_on_pause = True
        self.is_paused = True

    def cancel(self):
        # 1. إرسال إشارة فورية للواجهة لإزالة عنصر التحميل، مما يعطي إحساساً بالسرعة.
        self._send_update({"task_id": self.task_id, "type": "canceled"})

        # 2. تعيين الأعلام لإعلام جميع الخيوط بالتوقف.
        self.is_paused = False
        self.stop_flag.set()

        # 3. بدء خيط منفصل ومستقل للقيام بعملية التنظيف البطيئة في الخلفية.
        def background_cleanup():
            """ينتظر انتهاء الخيوط ويحذف الملفات دون تجميد الواجهة."""
            # أولاً، انتظر انتهاء خيط التحكم الرئيسي (run).
            # هذا يضمن أن جميع خيوط العمال قد تم إعلامها بالتوقف.
            if self.is_alive():
                self.join()
            # ثانياً، انتظر انتهاء جميع خيوط العمال.
            for t in self.worker_threads:
                if t.is_alive():
                    t.join()

            # تنظيف ملف الحالة
            state = load_state()
            if self.filepath in state:
                del state[self.filepath]
                save_state(state)
            # تنظيف الملفات المؤقتة والنهائية
            try:
                if os.path.exists(self.filepath): os.remove(self.filepath)
                num_segments = 1 if self.force_single_thread else NUM_SEGMENTS
                for i in range(num_segments):
                    part_file = self.filepath + f".part{i}"
                    if os.path.exists(part_file): os.remove(part_file)
            except OSError as e:
                print(f"Error during background file cleanup for {self.filepath}: {e}")
 
        # تشغيل التنظيف في خيط خفي (daemon) لا يمنع البرنامج من الإغلاق
        cleanup_thread = threading.Thread(target=background_cleanup, daemon=True, name=f"Cleanup-{self.task_id}")
        cleanup_thread.start()

    def _single_thread_download(self):
        """Handles downloads where the total size is unknown. Pause/resume is not supported."""
        try:
            last_update_time = time.monotonic()
            last_total_downloaded = 0
            with requests.get(self.url, stream=True, headers=self.http_headers, timeout=30, verify=False) as r:
                r.raise_for_status()
                with open(self.filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if self.stop_flag.is_set():
                            # Treat stop as cancellation since resuming is not possible.
                            self._send_update({"task_id": self.task_id, "type": "canceled"})
                            return
                        if chunk:
                            f.write(chunk)
                            current_total_downloaded = f.tell()
                            
                            current_time = time.monotonic()
                            if (current_time - last_update_time) >= 0.5:
                                speed = (current_total_downloaded - last_total_downloaded) / (current_time - last_update_time)
                                self.smoothed_speed = (speed * self.SMOOTHING_FACTOR) + (self.smoothed_speed * (1 - self.SMOOTHING_FACTOR))
                                
                                text = f"{os.path.basename(self.filepath)} - {format_size(current_total_downloaded)} | {format_size(self.smoothed_speed)}/s"
                                # Use percent=-1 to indicate an indeterminate progress bar
                                self._send_update({"task_id": self.task_id, "type": "progress", "percent": -1, "text": text})
                                
                                last_total_downloaded = current_total_downloaded
                                last_update_time = current_time
            final_size = os.path.getsize(self.filepath)
            text = f"{os.path.basename(self.filepath)} ✅ تم التحميل ({format_size(final_size)})"
            self._send_update({"task_id": self.task_id, "type": "done", "text": text})
        except Exception as e:
            self._send_update({"task_id": self.task_id, "type": "error", "text": f"❌ خطأ فادح: {e}"})

    def run(self):
        try:
            # If size is unknown, we can't do multi-part. Use a simple, single-threaded download.
            if self.total_size is None:
                self._single_thread_download()
                return

            was_paused = False
            num_segments = 1 if self.force_single_thread else NUM_SEGMENTS
            task_info = self.state.get(self.filepath)
            if isinstance(task_info, dict) and 'segments' in task_info and len(task_info['segments']) == num_segments:
                self.segment_progress = task_info['segments']
            else:
                self.segment_progress = [0] * num_segments

            # --- NEW: Smart Repair Logic ---
            # If in repair mode, force re-download of segments that are not fully downloaded.
            if self.repair_mode:
                segment_size_check = self.total_size // num_segments
                for i in range(num_segments - 1):
                    if self.segment_progress[i] < segment_size_check:
                        self.segment_progress[i] = 0 # Reset progress to force re-download

            segment_size = self.total_size // num_segments
            
            for i in range(num_segments):
                start = i * segment_size
                end = start + segment_size - 1
                if i == num_segments - 1: end = self.total_size - 1
                worker = threading.Thread(target=self.download_segment, args=(i, start, end))
                self.worker_threads.append(worker)
                worker.start()

            last_total_downloaded = sum(self.segment_progress)
            last_update_time = time.monotonic()

            while any(t.is_alive() for t in self.worker_threads):
                if self.stop_flag.is_set(): # Check stop_flag first for immediate response to cancel
                    # The stop_flag has been set. The worker threads will see this and exit on their own.
                    # The cleanup_thread (started by the cancel() method) is responsible for joining them.
                    # We just need to exit this monitoring loop immediately.
                    return

                if self.error_flag.is_set(): # Error takes precedence over pause
                    self.stop_flag.set()
                    for t in self.worker_threads: t.join()
                    # --- FIX: Save progress on error ---
                    # Save the current progress to the state file so it can be resumed later.
                    self.state[self.filepath] = {"url": self.url, "total_size": self.total_size, "segments": self.segment_progress}
                    save_state(self.state)
                    # --- END FIX ---
                    self._send_update({"task_id": self.task_id, "type": "error", "text": f"❌ خطأ: {self.error_message}"})
                    return

                if self.is_paused:
                    if not was_paused:
                        # This is the first moment we've detected the pause.
                        # Save state and update the UI to "Paused".
                        self.state[self.filepath] = {"url": self.url, "total_size": self.total_size, "segments": self.segment_progress}
                        save_state(self.state)
                        total_downloaded = sum(self.segment_progress)
                        text = f"{os.path.basename(self.filepath)} - متوقف مؤقتاً ({format_size(total_downloaded)} / {format_size(self.total_size)})"
                        self._send_update({"task_id": self.task_id, "type": "paused", "text": text})
                        was_paused = True
                    
                    if self.exit_on_pause:
                        return # Exit the thread cleanly after saving state

                    if self.exit_on_pause:
                        return # Exit the thread cleanly after saving state

                    time.sleep(0.5)
                    continue

                was_paused = False # Reset the flag once we are no longer paused.

                current_total_downloaded = sum(self.segment_progress)
                current_time = time.monotonic()
                if (current_time - last_update_time) >= 0.5:
                    speed = (current_total_downloaded - last_total_downloaded) / (current_time - last_update_time)
                    self.smoothed_speed = (speed * self.SMOOTHING_FACTOR) + (self.smoothed_speed * (1 - self.SMOOTHING_FACTOR))
                    eta = (self.total_size - current_total_downloaded) / self.smoothed_speed if self.smoothed_speed > 0 else None
                    percent = int((current_total_downloaded / self.total_size) * 100) if self.total_size > 0 else 0
                    text = f"{os.path.basename(self.filepath)} - {format_size(current_total_downloaded)} / {format_size(self.total_size)} ({percent}%) | {format_size(self.smoothed_speed)}/s | ETA: {format_eta(eta)}"
                    self._send_update({"task_id": self.task_id, "type": "progress", "percent": percent, "text": text})
                    last_total_downloaded = current_total_downloaded
                    last_update_time = current_time
                
                time.sleep(0.1)

            # --- FIX: Final check after all threads are done ---
            # This ensures that we don't proceed to combine files if the download
            # was incomplete due to an error or cancellation that wasn't caught.
            final_downloaded = sum(self.segment_progress)
            if not self.stop_flag.is_set() and final_downloaded < self.total_size:
                error_msg = f"خطأ: انتهت عملية التحميل قبل اكتمال الملف. ({format_size(final_downloaded)} / {format_size(self.total_size)})"
                self._send_update({"task_id": self.task_id, "type": "error", "text": error_msg})
                return

            self.combine_files()
            text = f"{os.path.basename(self.filepath)} ✅ تم التحميل ({format_size(self.total_size)})"
            self._send_update({"task_id": self.task_id, "type": "done", "text": text})
            if self.filepath in self.state:
                del self.state[self.filepath]
                save_state(self.state)

        except Exception as e:
            self._send_update({"task_id": self.task_id, "type": "error", "text": f"❌ خطأ فادح: {e}"})

    def download_segment(self, index, start_byte, end_byte):
        part_file = self.filepath + f".part{index}"
        max_retries = 3
        for attempt in range(max_retries):
            if self.stop_flag.is_set(): return
            downloaded = self.segment_progress[index]
            current_pos = start_byte + downloaded
            if current_pos > end_byte: return

            headers = {'Range': f'bytes={current_pos}-{end_byte}'}
            headers.update(self.http_headers)
            mode = 'ab' if downloaded > 0 else 'wb'

            try:
                with requests.get(self.url, stream=True, headers=headers, timeout=60, verify=False) as r:
                    r.raise_for_status()
                    with open(part_file, mode) as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if self.stop_flag.is_set(): return
                            if chunk:
                                f.write(chunk)
                                self.segment_progress[index] += len(chunk)
                return
            except requests.exceptions.RequestException as e:
                # إذا تم إيقاف الخيط، لا تعتبره خطأً.
                if self.stop_flag.is_set(): return
                print(f"Segment {index} error (Attempt {attempt + 1}): {e}")
                # Add specific handling for 403 Forbidden errors, which often block multi-part downloads.
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 403:
                    self.error_message = "تم رفض الوصول (403). الخادم يمنع هذا النوع من التحميل."
                    self.error_flag.set()
                    return
                if attempt == max_retries - 1:
                    self.error_message = f"فشل الاتصال بالخادم: {e}"
                    self.error_flag.set()
                    return
                time.sleep(5)

    def combine_files(self):
        num_segments = 1 if self.force_single_thread else NUM_SEGMENTS
        if num_segments == 1:
            part_file = self.filepath + ".part0"
            if os.path.exists(part_file): os.rename(part_file, self.filepath)
            return

        with open(self.filepath, 'wb') as final_file:
            for i in range(num_segments):
                part_file = self.filepath + f".part{i}"
                if os.path.exists(part_file):
                    with open(part_file, 'rb') as pf:
                        shutil.copyfileobj(pf, final_file)
                    os.remove(part_file)

# --- Yt-dlp Integration ---

class YTDLRunner(threading.Thread):
    """A thread to run a yt-dlp download process and report progress via a queue."""
    def __init__(self, task_id, url, download_folder, update_queue=None, format_id=None, audio_only=False, update_callback=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.task_id = task_id
        self.url = url
        self.download_folder = download_folder
        self.update_queue = update_queue
        self.update_callback = update_callback
        self.format_id = format_id
        self.audio_only = audio_only
        self.total_size = 0 # --- إصلاح: إضافة متغير لتخزين الحجم الكلي ---
        # --- Flags for pause/resume simulation ---
        self.stop_flag = threading.Event()
        self.pause_flag = threading.Event()
        self.resume_flag = threading.Event()
        self.is_paused = False
        # --- متغيرات لحساب السرعة يدوياً ---
        self.last_update_time = 0
        self.last_downloaded_bytes = 0
        self.smoothed_speed = 0.0
        # --- جديد: متغيرات لتتبع تقدم الفيديو والصوت بشكل منفصل ---
        self.video_bytes = 0
        self.audio_bytes = 0
        self.video_total = 0
        self.audio_total = 0
        self.SMOOTHING_FACTOR = 0.2
        # --- إصلاح: إضافة متغير http_headers المفقود ---
        # هذا يحل خطأ AttributeError ويسمح بتمرير هيدرز مخصصة إذا لزم الأمر.
        self.http_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': url,
        }

    def _send_update(self, data):
        """Helper to send updates via queue or callback."""
        if self.update_callback:
            self.update_callback(data)
        elif self.update_queue:
            self.update_queue.put(data)

    def toggle_pause_resume(self):
        """
        يحاكي الإيقاف المؤقت والاستئناف لـ yt-dlp.
        هذا لا يوقف التحميل الحالي، بل يمنع بدء تحميل جديد عند الاستئناف.
        """
        if self.is_paused:
            # إذا كان متوقفاً، قم بتفعيل علم الاستئناف
            self.resume_flag.set()
        else:
            # إذا كان يعمل، قم بتفعيل علم الإيقاف المؤقت
            self.pause_flag.set()

    def _hook(self, d):
        # --- إصلاح: التحقق من الإلغاء والإيقاف المؤقت في بداية الدالة لضمان الاستجابة الفورية ---
        if self.stop_flag.is_set():
            # هذا يوقف التحميل فوراً عند طلب الإلغاء
            raise yt_dlp.utils.DownloadError("Download cancelled by user.")

        # This is the core of the pause simulation.
        if self.pause_flag.is_set():
            raise yt_dlp.utils.DownloadError("Download paused by user.")

        if d['status'] == 'downloading':
            # --- إصلاح: تتبع تقدم الفيديو والصوت بشكل منفصل لتوحيد شريط التقدم ---
            info_dict = d.get('info_dict', {})
            
            # تحديد ما إذا كان الجزء الحالي هو فيديو أم صوت
            is_video = info_dict.get('vcodec', 'none') != 'none'
            
            if is_video:
                self.video_bytes = d.get('downloaded_bytes', 0)
                if not self.video_total: self.video_total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
            else: # It's audio
                self.audio_bytes = d.get('downloaded_bytes', 0)
                if not self.audio_total: self.audio_total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)

            total_bytes = self.video_total + self.audio_total
            downloaded_bytes = self.video_bytes + self.audio_bytes
            if total_bytes > 0: self.total_size = total_bytes

            percent = 0
            if total_bytes > 0 and downloaded_bytes is not None: percent = (downloaded_bytes / total_bytes) * 100

            # --- إصلاح: تنظيف النص من أكواد الألوان (ANSI escape codes) ---
            # yt-dlp يرسل أكواد تلوين، يجب إزالتها قبل عرضها في الواجهة.
            ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

            filename = d.get('info_dict', {}).get('title', '...')
            eta_str = d.get('eta_str', d.get('_eta_str', ''))
            try:
                # --- الحل النهائي لمشكلة KiB/MiB: حساب السرعة يدوياً ---
                current_time = time.monotonic()
                elapsed_time = current_time - self.last_update_time
                
                # نحدث الواجهة كل نصف ثانية لتجنب إرهاقها
                if elapsed_time >= 0.5:
                    # 1. حساب السرعة الحالية بالبايت/ثانية
                    speed = (downloaded_bytes - self.last_downloaded_bytes) / elapsed_time
                    # 2. تنعيم قيمة السرعة لتكون أكثر استقراراً
                    self.smoothed_speed = (speed * self.SMOOTHING_FACTOR) + (self.smoothed_speed * (1 - self.SMOOTHING_FACTOR))
                    # 3. تنسيق السرعة باستخدام دالة format_size الصحيحة
                    speed_str_formatted = f"{format_size(self.smoothed_speed)}/s"

                    # 4. تحديث المتغيرات للمرة القادمة
                    self.last_update_time = current_time
                    self.last_downloaded_bytes = downloaded_bytes

                    # 5. بناء النص النهائي بالوحدات الصحيحة
                    # --- إصلاح: إعادة ترتيب النص ليظهر بالشكل الصحيح ---
                    clean_eta = ansi_escape.sub('', eta_str).replace('[K', '').strip()
                    detailed_text = f"{filename} - {format_size(downloaded_bytes)} / {format_size(total_bytes)} ({int(percent)}%) | {speed_str_formatted} | ETA: {clean_eta}"
                    self._send_update({"task_id": self.task_id, "type": "progress", "percent": percent, "text": detailed_text})
            except Exception as e: pass # تجاهل الأخطاء البسيطة في التنسيق


    def run(self):
        while not self.stop_flag.is_set():
            if not YTDLP_AVAILABLE:
                self._send_update({"task_id": self.task_id, "type": "error", "text": "❌ مكتبة yt-dlp غير مثبتة."})
                return

            # Reset flags for the new download attempt
            self.pause_flag.clear()
            self.resume_flag.clear()
            self.is_paused = False

            if self.audio_only:
                # --- جديد: إعدادات خاصة لتحميل الصوت فقط بصيغة MP3 ---
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'outtmpl': os.path.join(self.download_folder, '%(title)s.%(ext)s'),
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192', # جودة 192kbps تعتبر جيدة جداً
                    }],
                    # --- إصلاح: ضمان حذف الملف الأصلي بعد تحويله إلى MP3 ---
                    'keepvideo': False,
                    'progress_hooks': [self._hook],
                    'noplaylist': True, 'nocheckcertificate': True, 'restrictfilenames': True,
                    'nooverwrites': True,
                }
            else:
                # --- تعديل: منطق مخصص لتحميل الفيديو من TikTok ---
                # --- إصلاح: تعريف المتغيرات قبل استخدامها لتجنب خطأ UnboundLocalError ---
                # --- تحسين: التحقق من الرابط الأصلي والرابط النهائي لـ Dailymotion ---
                # هذا يضمن تطبيق التحسينات حتى لو كان الرابط مختصراً (مثل dai.ly).
                # --- إصلاح: إنشاء كائن ydl قبل استخدامه لجلب المعلومات ---
                # هذا يحل خطأ UnboundLocalError.
                # --- إصلاح: التحقق من وجود ffmpeg قبل تحديد صيغة التحميل ---
                ffmpeg_available = shutil.which('ffmpeg') is not None

                # إذا كان ffmpeg غير متاح، نطلب دائماً أفضل صيغة مدمجة لتجنب خطأ الدمج.
                if not ffmpeg_available:
                    video_format = 'best[ext=mp4]/best'
                else:
                    # إذا كان ffmpeg متاحاً، نستخدم المنطق القديم لاختيار أفضل جودة.
                    with yt_dlp.YoutubeDL({'quiet': True, 'noplaylist': True, 'nocheckcertificate': True}) as ydl:
                        info_dict = ydl.extract_info(self.url, download=False)

                    webpage_url = info_dict.get('webpage_url', self.url) if info_dict else self.url
                    is_dailymotion_url = 'dailymotion.com' in webpage_url.lower() or 'dai.ly' in self.url.lower()

                    if is_dailymotion_url:
                        video_format = 'best[height<=480][protocol^=http]/best[height<=480]/best[protocol^=http]/best'
                    else:
                        video_format = 'bestvideo+bestaudio/best'

                # --- الحل النهائي والمحسّن لمشكلة Dailymotion بناءً على اقتراحاتك ---
                # ندمج طلب الجودة مع طلب بروتوكول http لتجنب بث HLS الذي يسبب مشاكل في تقدير الحجم.
                if self.format_id:
                    # للمواقع الأخرى، نستخدم المنطق القديم.
                    final_format_string = f"{self.format_id}+bestaudio[ext=m4a]/bestaudio"
                else:
                    # إذا لم يختر المستخدم جودة، نستخدم الصيغة الافتراضية التي حددناها.
                    final_format_string = video_format
                
                ydl_opts = {
                    'format': final_format_string,
                    # --- إصلاح: ضمان حفظ الملف باسمه الأصلي على يوتيوب ---
                    # استخدام '%(title)s.%(ext)s' يضمن أن اسم الملف سيكون هو عنوان الفيديو.
                    # restrictfilenames=True يقوم بتنظيف الاسم من أي رموز غير صالحة في أسماء الملفات.
                    # --- إصلاح: منع الكتابة فوق الملفات الموجودة ---
                    # هذا الخيار يخبر yt-dlp بالبحث عن اسم ملف غير مستخدم (مثل video (2).mp4)
                    # إذا كان الاسم الأصلي موجوداً بالفعل.
                    'outtmpl': os.path.join(self.download_folder, '%(title)s.%(ext)s'),
                    'nopart': True, # Avoid .part files
                    'nooverwrites': True, # Don't overwrite existing files (yt-dlp will add (2), (3), etc.)
                    'forcefilename': False, # Allow yt-dlp to pick a new name

                    'http_headers': self.http_headers,
                    'progress_hooks': [self._hook],
                    'noplaylist': True, 'nocheckcertificate': True, 'restrictfilenames': True,
                    'merge_output_format': 'mp4',
                    'postprocessor_args': {'Merger': ['-movflags', 'faststart']},
                    'continuedl': True,
                    }


            # --- الحل الجذري لمشكلة TikTok: محاولة التحميل المباشر كخطة بديلة ---
            is_tiktok_url = 'tiktok.com' in self.url
            if is_tiktok_url:
                # --- إصلاح: تخزين آخر معلومات تم جمعها لاستخدامها عند الإلغاء ---
                try:
                    with yt_dlp.YoutubeDL({**ydl_opts, 'outtmpl': os.path.join(self.download_folder, info_dict.get('title', 'video') + '.%(ext)s')}) as ydl:

                        ydl.download([self.url])
                    # إذا نجح، اخرج من الحلقة
                    break
                except Exception as ytdl_error:
                    # إذا فشل yt-dlp، جرب الطريقة اليدوية
                    self._manual_tiktok_download()
                    # سواء نجحت الطريقة اليدوية أو فشلت، اخرج من الحلقة لأننا حاولنا كل شيء
                    break

            # --- المنطق الأصلي للمواقع الأخرى ---
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # 1. قم بالتحميل. الدالة ستنتظر حتى يكتمل كل شيء.
                    # --- إصلاح: تخزين آخر معلومات تم جمعها لاستخدامها عند الإلغاء ---
                    self._last_info_dict = ydl.extract_info(self.url, download=True)

            # --- إصلاح: نقل منطق إرسال إشارة الاكتمال إلى خارج كتلة try/except ---
            # هذا يضمن أن الإشارة تُرسل فقط بعد التأكد من أن التحميل لم يتوقف بسبب خطأ.
            except yt_dlp.utils.DownloadError as e:
                # --- جديد: التعامل مع خطأ ffmpeg بشكل خاص ---
                if "ffmpeg is not installed" in str(e):
                    error_text = "❌ خطأ: يتطلب هذا الفيديو ffmpeg للدمج. يرجى تثبيته أو اختيار صيغة أخرى."
                    self._send_update({"task_id": self.task_id, "type": "error", "text": error_text})
                    break

                if "Download paused by user" in str(e):
                    self.is_paused = True
                    self._send_update({"task_id": self.task_id, "type": "paused", "text": "متوقف مؤقتاً"})
                    # Wait here until resume is triggered
                    self.resume_flag.wait()
                    self._send_update({"task_id": self.task_id, "type": "resumed"})
                    continue # Go to the start of the while loop to re-download
                # --- إصلاح: التحقق من الإلغاء من خلال stop_flag لضمان الموثوقية ---
                elif self.stop_flag.is_set():
                    break # Exit loop on cancellation
                else: # Handle other download errors
                    error_text = f"❌ خطأ من yt-dlp: {str(e)}"
                    self._send_update({"task_id": self.task_id, "type": "error", "text": error_text[:250]})
                    break # Exit loop on other errors
            except Exception as e:
                # Catch any other unexpected errors during download
                error_text = f"❌ خطأ غير متوقع في yt-dlp: {str(e)}"
                self._send_update({"task_id": self.task_id, "type": "error", "text": error_text[:250]})
                break
            else:
                # إذا اكتمل التحميل بنجاح، اخرج من الحلقة
                break
        
        # --- إصلاح: إرسال إشارة الاكتمال النهائية بعد الخروج من حلقة التحميل بنجاح ---
        # هذا يضمن أن الواجهة يتم تحديثها بشكل صحيح بعد اكتمال جميع العمليات.
        if not self.stop_flag.is_set() and not self.is_paused:
            # --- إصلاح جذري: التحقق من وجود معلومات قبل محاولة الوصول إليها ---
            # هذا يمنع حدوث خطأ 'NoneType' object has no attribute 'get'
            last_info = getattr(self, '_last_info_dict', None)
            if last_info:
                # بعد انتهاء التحميل، احصل على المسار النهائي للملف من yt-dlp.
                final_filepath = self._get_final_filepath_from_info(last_info)

                if final_filepath and os.path.exists(final_filepath):
                    display_title = os.path.basename(final_filepath)
                    text = f"✅ تم تحميل {display_title}"
                    self._send_update({"task_id": self.task_id, "type": "done", "text": text})
                else:
                    # حالة احتياطية إذا لم يتم العثور على المسار
                    self._send_update({"task_id": self.task_id, "type": "done", "text": "✅ تم التحميل بنجاح"})
            else:
                # إذا لم يتم تعيين last_info، فهذا يعني أن التحميل فشل في مرحلة مبكرة.
                # لا نرسل إشارة "done" لأن إشارة "error" يجب أن تكون قد أُرسلت بالفعل.
                pass

    def _manual_tiktok_download(self):
        """
        A fallback method to download TikTok videos by scraping the page directly.
        This is used when yt-dlp fails.
        """
        if not CLOUDSCRAPER_AVAILABLE:
            self._send_update({"task_id": self.task_id, "type": "error", "text": "❌ فشل التحميل. يرجى تثبيت مكتبة cloudscraper."})
            return

        try:
            self._send_update({"task_id": self.task_id, "type": "progress", "percent": -1, "text": "فشل yt-dlp. جاري محاولة التحميل المباشر..."})
            scraper = cloudscraper.create_scraper()
            response = scraper.get(self.url, timeout=20)
            response.raise_for_status()

            # Find video URL in the page source
            match = re.search(r'"playAddr":"([^"]+)"', response.text)
            if not match:
                self._send_update({"task_id": self.task_id, "type": "error", "text": "❌ فشل العثور على رابط الفيديو في الصفحة."})
                return

            video_url = match.group(1).encode().decode('unicode_escape')

            # Find video title
            title_match = re.search(r'<title>(.*?)</title>', response.text)
            title = title_match.group(1).split(' | TikTok')[0] if title_match else f"tiktok_video_{self.task_id}"
            safe_title = re.sub(r'[\\/*?:"<>|]', "", title)
            filepath = os.path.join(self.download_folder, f"{safe_title}.mp4")

            # Download the video using requests
            video_response = requests.get(video_url, stream=True, headers={'Referer': 'https://www.tiktok.com/'})
            video_response.raise_for_status()
            total_size = int(video_response.headers.get('content-length', 0))

            downloaded_bytes = 0
            with open(filepath, 'wb') as f:
                for chunk in video_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        percent = (downloaded_bytes / total_size * 100) if total_size > 0 else 0
                        self._send_update({"task_id": self.task_id, "type": "progress", "percent": percent, "text": f"جاري التحميل المباشر... {int(percent)}%"})
            # --- إصلاح: إرسال إشارة الاكتمال الصحيحة للواجهة ---
            # هذا يضمن أن الواجهة ستعرف أن التحميل قد انتهى بنجاح.
            final_text = f"✅ تم تحميل {safe_title} بنجاح"
            self._send_update({"task_id": self.task_id, "type": "done", "text": final_text})
            # --- نهاية الإصلاح ---

        except Exception as e:
            self._send_update({"task_id": self.task_id, "type": "error", "text": f"❌ فشل التحميل المباشر: {e}"})

    def _get_final_filepath_from_info(self, info_dict):
        """Helper to reliably get the final file path from the info dict."""
        return info_dict.get('requested_downloads', [{}])[0].get('filepath') or info_dict.get('filepath')

    def cancel(self):
        """Stops the download and cleans up the partially downloaded file."""
        self.stop_flag.set()

        def background_cleanup():
            """Waits for the thread to finish and then deletes the file."""
            if self.is_alive():
                self.join()

            # --- إصلاح: منطق جديد وموثوق لحذف ملفات yt-dlp عند الإلغاء ---
            try:
                # بعد توقف الخيط، نحاول الحصول على مسار الملف من آخر معلومات تم جمعها.
                # هذا هو المكان الذي يتم فيه تخزين المسار المؤقت أو النهائي.
                if hasattr(self, '_last_info_dict'):
                    filepath_to_delete = self._get_final_filepath_from_info(self._last_info_dict)
                    if filepath_to_delete and os.path.exists(filepath_to_delete):
                        os.remove(filepath_to_delete)
                        print(f"Cleaned up cancelled yt-dlp file: {filepath_to_delete}")

            except Exception as e:
                print(f"Error during yt-dlp cleanup: {e}")

        cleanup_thread = threading.Thread(target=background_cleanup, daemon=True, name=f"Cleanup-YTDL-{self.task_id}")
        cleanup_thread.start()

def get_yt_dlp_info(url, settings=None):
    if not YTDLP_AVAILABLE: return None, "مكتبة yt-dlp غير مثبتة."
    
    settings = settings or {}
    # Custom logger to capture warnings and errors from yt-dlp.
    class YTDLLogger:
        def __init__(self):
            self.warnings = []
            self.errors = []
        def debug(self, msg):
            # تجاهل رسائل تصحيح الأخطاء الطويلة المتعلقة بـ cookies
            if 'cookie' in msg.lower():
                return
            pass            
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): self.warnings.append(msg)
        def error(self, msg):
            # yt-dlp sometimes prefixes errors with "ERROR: ". We can strip that.
            if msg.startswith('ERROR: '):
                msg = msg[7:]
            self.errors.append(msg)

    logger = YTDLLogger()
    base_opts = {
        'quiet': True, 'noplaylist': True, 'nocheckcertificate': True,
        'skip_download': True, 'logger': logger
    }

    # --- الحل: إضافة دعم ملفات تعريف الارتباط (Cookies) ---
    # هذا يحل مشكلة "Sign in to confirm you’re not a bot" من يوتيوب.
    cookies_file = settings.get("youtube_cookies_path")
    if cookies_file and os.path.exists(cookies_file):
        base_opts['cookies'] = cookies_file

    # --- First attempt: Standard info extraction ---
    try:
        with yt_dlp.YoutubeDL(base_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if info:
                return info, None # Success on the first try
    except yt_dlp.utils.DownloadError as e:
        # Check if the error is the specific "no video" error for Instagram.
        # If so, we'll fall through to the next attempt. Otherwise, it's a real error.
        if "There is no video in this post" not in str(e):
            return None, str(e)
    except Exception as e:
        return None, str(e)

    # --- Second attempt (if first failed): For Instagram images ---
    # This is triggered if the first attempt failed with "no video" error.
    # We try again, telling yt-dlp not to look for videos.
    if any("There is no video in this post" in err for err in logger.errors):
        try:
            image_opts = base_opts.copy()
            # This option is not a standard yt-dlp option, but we can simulate it
            # by how we handle the result. The key is that the first attempt failed.
            # The logic in main_pyside.py already handles image-only results.
            # Let's re-run with the same options, but check the logger output.
            with yt_dlp.YoutubeDL(base_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info:
                    return info, None
        except Exception as e:
            # If this also fails, return the original error.
            return None, str(e)

    # --- Final fallback: If info is still None, check captured logs for a clear message ---
    try:
        with yt_dlp.YoutubeDL(base_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                for w in logger.warnings:
                    if 'returned nothing' in w:
                        return None, "لم يتم العثور على فيديو في الرابط. تأكد أن الرابط عام وصحيح."
                return None, "فشل استخراج المعلومات. قد يكون الرابط غير مدعوم أو خاص."
            return info, None
    except Exception as e:
        # Return the most relevant error from the logger if available
        if logger.errors:
            return None, logger.errors[0]
        return None, str(e)

def get_yt_dlp_playlist_info(url):
    """
    Extracts a list of entries from a playlist URL (like a Telegram channel)
    without downloading or extracting deep info for each entry.
    """
    if not YTDLP_AVAILABLE:
        return None, "مكتبة yt-dlp غير مثبتة."

    class YTDLLogger:
        def __init__(self):
            self.warnings = []
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): self.warnings.append(msg)
        def error(self, msg): print(f"YTDL-PLAYLIST-ERROR: {msg}")

    logger = YTDLLogger()
    ydl_opts = {
        'quiet': True,
        'nocheckcertificate': True,
        'logger': logger
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info, None
    except Exception as e:
        return None, f"فشل في جلب قائمة التشغيل: {e}"

def get_yt_dlp_playlist_entries(url, callback):
    """
    Extracts entries from a playlist URL and yields them one by one via a callback.
    This is for progressive loading in the UI.
    `callback`: A function to be called for each video entry found.
    Returns the playlist title on success, or raises an exception on failure.
    """
    if not YTDLP_AVAILABLE:
        raise Exception("مكتبة yt-dlp غير مثبتة.")

    class YTDLLogger:
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): print(f"YTDL-PLAYLIST-ERROR: {msg}")

    ydl_opts = {
        'quiet': True,
        'nocheckcertificate': True,
        'extract_flat': 'in_playlist', # يجلب قائمة الفيديوهات بسرعة فائقة دون تفاصيل عميقة
        'force_generic_extractor': True, # يضمن أن 'entries' ستكون generator
        'logger': YTDLLogger(),
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # استخراج المعلومات الأولية (بدون معالجة الفيديوهات الداخلية)
            playlist_info = ydl.extract_info(url, download=False)

            if 'entries' not in playlist_info:
                raise Exception("لم يتم العثور على فيديوهات في هذا الرابط.")

            # الآن، قم بالمرور على كل فيديو في القائمة (generator)
            for entry in playlist_info['entries']:
                callback(entry) # إرسال كل فيديو على حدة للواجهة
            
            return playlist_info.get('title', 'Playlist')
    except Exception as e:
        # إعادة إرسال الخطأ ليتم التعامل معه في العامل (worker)
        raise e

# --- جديد: نقل العامل إلى الملف المشترك ---
# هذا العامل أصبح الآن متاحاً لكل من main_pyside.py و web_main.py
class TelethonDirectFetcher(threading.Thread):
    """
    Uses Telethon to fetch a single, specific message from a Telegram channel.
    Modified to work as a standard thread and use a callback for results.
    """
    def __init__(self, url, api_id, api_hash, session_string, result_callback, error_callback):
        super().__init__(daemon=True)
        self.url = url
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.result_callback = result_callback
        self.error_callback = error_callback

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = None
        try:
            client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash, loop=loop)

            async def do_fetch_single():
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("جلسة Telethon غير صالحة. يرجى تسجيل الدخول من الإعدادات.")

                parsed_url = urlparse(self.url)
                path_parts = parsed_url.path.strip('/').split('/')
                if len(path_parts) < 2:
                    raise ValueError("رابط المنشور غير صالح. يجب أن يكون بالصيغة t.me/channel/id")
                
                channel_ref, msg_id = path_parts[0], int(path_parts[1])
                channel_entity = await client.get_entity(channel_ref)
                message = await client.get_messages(channel_entity, ids=msg_id)

                if not message or not (message.file or message.photo):
                    raise Exception("لم يتم العثور على ملف أو صورة في هذا المنشور.")

                media_obj = message.photo or message.file
                filename = getattr(media_obj, 'name', f"telegram_photo_{msg_id}.jpg")
                total_size = getattr(media_obj, 'size', None)
                internal_url = f"telethon://{getattr(channel_entity, 'username', channel_ref)}/{msg_id}"
                
                self.result_callback(internal_url, filename, total_size)

            loop.run_until_complete(do_fetch_single())
        except Exception as e:
            self.error_callback(f"خطأ من Telethon: {e}")
        finally:
            if client and client.is_connected():
                loop.run_until_complete(client.disconnect())
            loop.close()

# --- جديد: إضافة عمال الأدوات إلى الملف الأساسي ---

class ImageConverterWorker(threading.Thread):
    def __init__(self, input_path, output_path, output_format, update_callback=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.input_path = input_path
        self.output_path = output_path
        self.output_format = output_format
        self.update_callback = update_callback

    def run(self):
        try:
            if not PIL_AVAILABLE:
                raise Exception("مكتبة Pillow غير مثبتة (pip install Pillow).")

            with Image.open(self.input_path) as img:
                output_format_upper = self.output_format.upper()
                if output_format_upper in ['JPG', 'JPEG', 'BMP'] and (img.mode == 'RGBA' or 'transparency' in img.info):
                    background = Image.new("RGB", img.size, (255, 255, 255))
                    background.paste(img, mask=img.split()[3]) # Use alpha channel as mask
                    background.save(self.output_path, format=output_format_upper)
                else:
                    img.save(self.output_path, format=output_format_upper)
            
            if self.update_callback:
                self.update_callback({"status": "success", "message": f"✅ تم الحفظ في: {self.output_path}"})
        except Exception as e:
            if self.update_callback:
                self.update_callback({"status": "error", "message": f"❌ خطأ: {e}"})

class VideoMergerWorker(threading.Thread):
    def __init__(self, video_path, audio_path, output_path, update_callback=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.video_path = video_path
        self.audio_path = audio_path
        self.output_path = output_path
        self.update_callback = update_callback

    def run(self):
        try:
            command = ['ffmpeg', '-i', self.video_path, '-i', self.audio_path, '-c:v', 'copy', '-c:a', 'copy', '-shortest', '-movflags', 'faststart', self.output_path]
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
            process = subprocess.run(command, check=True, capture_output=True, text=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')
            if self.update_callback:
                self.update_callback({"status": "success", "message": f"✅ تم الدمج بنجاح! تم الحفظ في:\n{self.output_path}"})
        except subprocess.CalledProcessError as e:
            if self.update_callback:
                self.update_callback({"status": "error", "message": f"❌ فشل الدمج:\n{e.stderr}"})
        except Exception as e:
            if self.update_callback:
                self.update_callback({"status": "error", "message": f"❌ خطأ غير متوقع: {e}"})

class FileRepairWorker(threading.Thread):
    def __init__(self, input_path, output_path, deep_repair, update_callback=None, **kwargs):
        super().__init__(daemon=True, **kwargs)
        self.input_path = input_path
        self.output_path = output_path
        self.deep_repair = deep_repair
        self.update_callback = update_callback

    def run(self):
        try:
            # Get duration for progress calculation
            duration_cmd = ['ffmpeg', '-i', self.input_path]
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
            result = subprocess.run(duration_cmd, capture_output=True, text=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')
            duration_str = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2})\.\d+", result.stderr)
            total_duration_sec = 0
            if duration_str:
                h, m, s = map(int, duration_str.groups())
                total_duration_sec = h * 3600 + m * 60 + s

            # Main ffmpeg command
            if self.deep_repair:
                command = ['ffmpeg', '-y', '-i', self.input_path, '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23', '-c:a', 'aac', '-b:a', '192k', self.output_path]
                success_message = f"✅ تم الإصلاح العميق بنجاح! تم الحفظ في:\n{self.output_path}"
            else:
                command = ['ffmpeg', '-y', '-i', self.input_path, '-c', 'copy', self.output_path]
                success_message = f"✅ تم الإصلاح السريع بنجاح! تم الحفظ في:\n{self.output_path}"
            
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')

            for line in process.stdout:
                if "time=" in line and total_duration_sec > 0 and self.update_callback:
                    time_str = re.search(r"time=(\d{2}):(\d{2}):(\d{2})\.\d+", line)
                    if time_str:
                        h, m, s = map(int, time_str.groups())
                        current_sec = h * 3600 + m * 60 + s
                        percent = int((current_sec / total_duration_sec) * 100)
                        self.update_callback({"status": "progress", "percent": percent, "text": f"جاري الإصلاح... {percent}%"})
            
            process.wait()

            if process.returncode != 0:
                raise Exception("فشل ffmpeg. قد يكون الملف تالفاً بشدة.")
            
            if self.update_callback:
                self.update_callback({"status": "success", "message": success_message})

        except Exception as e:
            if self.update_callback:
                self.update_callback({"status": "error", "message": f"❌ خطأ غير متوقع: {e}"})

def get_yt_dlp_info(url):
    if not YTDLP_AVAILABLE: return None, "مكتبة yt-dlp غير مثبتة."
    
    # Custom logger to capture warnings and errors from yt-dlp.
    class YTDLLogger:
        def __init__(self):
            self.warnings = []
            self.errors = []
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): self.warnings.append(msg)
        def error(self, msg):
            # yt-dlp sometimes prefixes errors with "ERROR: ". We can strip that.
            if msg.startswith('ERROR: '):
                msg = msg[7:]
            self.errors.append(msg)

    logger = YTDLLogger()
    base_opts = {
        'quiet': True, 'noplaylist': True, 'nocheckcertificate': True,
        'skip_download': True, 'logger': logger
    }

    # --- First attempt: Standard info extraction ---
    try:
        with yt_dlp.YoutubeDL(base_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if info:
                return info, None # Success on the first try
    except yt_dlp.utils.DownloadError as e:
        # Check if the error is the specific "no video" error for Instagram.
        # If so, we'll fall through to the next attempt. Otherwise, it's a real error.
        if "There is no video in this post" not in str(e):
            return None, str(e)
    except Exception as e:
        return None, str(e)

    # --- Second attempt (if first failed): For Instagram images ---
    # This is triggered if the first attempt failed with "no video" error.
    # We try again, telling yt-dlp not to look for videos.
    if any("There is no video in this post" in err for err in logger.errors):
        try:
            image_opts = base_opts.copy()
            # This option is not a standard yt-dlp option, but we can simulate it
            # by how we handle the result. The key is that the first attempt failed.
            # The logic in main_pyside.py already handles image-only results.
            # Let's re-run with the same options, but check the logger output.
            with yt_dlp.YoutubeDL(base_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info:
                    return info, None
        except Exception as e:
            # If this also fails, return the original error.
            return None, str(e)

    # --- Final fallback: If info is still None, check captured logs for a clear message ---
    try:
        with yt_dlp.YoutubeDL(base_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                for w in logger.warnings:
                    if 'returned nothing' in w:
                        return None, "لم يتم العثور على فيديو في الرابط. تأكد أن الرابط عام وصحيح."
                return None, "فشل استخراج المعلومات. قد يكون الرابط غير مدعوم أو خاص."
            return info, None
    except Exception as e:
        # Return the most relevant error from the logger if available
        if logger.errors:
            return None, logger.errors[0]
        return None, str(e)

def get_yt_dlp_playlist_info(url):
    """
    Extracts a list of entries from a playlist URL (like a Telegram channel)
    without downloading or extracting deep info for each entry.
    """
    if not YTDLP_AVAILABLE:
        return None, "مكتبة yt-dlp غير مثبتة."

    class YTDLLogger:
        def __init__(self):
            self.warnings = []
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): self.warnings.append(msg)
        def error(self, msg): print(f"YTDL-PLAYLIST-ERROR: {msg}")

    logger = YTDLLogger()
    ydl_opts = {
        'quiet': True,
        'nocheckcertificate': True,
        'logger': logger
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info, None
    except Exception as e:
        return None, f"فشل في جلب قائمة التشغيل: {e}"

def get_yt_dlp_playlist_entries(url, callback):
    """
    Extracts entries from a playlist URL and yields them one by one via a callback.
    This is for progressive loading in the UI.
    `callback`: A function to be called for each video entry found.
    Returns the playlist title on success, or raises an exception on failure.
    """
    if not YTDLP_AVAILABLE:
        raise Exception("مكتبة yt-dlp غير مثبتة.")

    class YTDLLogger:
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): print(f"YTDL-PLAYLIST-ERROR: {msg}")

    ydl_opts = {
        'quiet': True,
        'nocheckcertificate': True,
        'extract_flat': 'in_playlist', # يجلب قائمة الفيديوهات بسرعة فائقة دون تفاصيل عميقة
        'force_generic_extractor': True, # يضمن أن 'entries' ستكون generator
        'logger': YTDLLogger(),
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # استخراج المعلومات الأولية (بدون معالجة الفيديوهات الداخلية)
            playlist_info = ydl.extract_info(url, download=False)

            if 'entries' not in playlist_info:
                raise Exception("لم يتم العثور على فيديوهات في هذا الرابط.")

            # الآن، قم بالمرور على كل فيديو في القائمة (generator)
            for entry in playlist_info['entries']:
                callback(entry) # إرسال كل فيديو على حدة للواجهة
            
            return playlist_info.get('title', 'Playlist')
    except Exception as e:
        # إعادة إرسال الخطأ ليتم التعامل معه في العامل (worker)
        raise e
