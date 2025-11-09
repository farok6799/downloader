import sys
from PySide6.QtCore import QRunnable, Slot
import os
import time
from urllib.parse import urlparse
import math
import queue
import threading
import re
import shutil
import zipfile, requests, subprocess
import urllib3
import json
import asyncio

# Suppress only the single InsecureRequestWarning from urllib3 needed for verify=False.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- فحص واستيراد مكتبة Telethon ---
try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

# --- استيراد منطق التحميل الأساسي ---
# هذا هو "العقل" الذي يحتوي على ميزات التحميل متعدد الأجزاء والإيقاف المؤقت
from downloader_core import DownloadTask, get_download_details, YTDLRunner, get_yt_dlp_info, get_yt_dlp_playlist_info, get_yt_dlp_playlist_entries, YTDLP_AVAILABLE, load_state, save_state, TelethonDirectFetcher

# --- استيراد مدير تيليجرام الجديد ---
from telegram_manager import TelegramManager, run_async_from_sync

# --- جديد: استيراد خادم البث المحلي ---
from local_stream_server import LocalStreamServer

# --- استيراد مكونات الشبكة والتكامل مع النظام ---
from PySide6.QtNetwork import QLocalServer, QLocalSocket

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QProgressBar, QLabel, QScrollArea, QFrame, QDialog,
    QTreeWidget, QTreeWidgetItem, QHeaderView, QMessageBox, QTabWidget, QSlider, QListWidget, QStackedLayout, QCheckBox, QInputDialog, QSpinBox, QAbstractItemView,
    QFileDialog, QComboBox, QSpacerItem, QSizePolicy, QListWidgetItem, QStyle, QMenu
)
from PySide6.QtCore import (
    QObject, Signal, Slot, QRunnable, QThreadPool, Qt, QTimer, QEvent, QUrl, QSize
)
from PySide6.QtGui import (
    QIcon, QImage, QPixmap, QCursor
)
# --- فحص المكتبات الإضافية ---
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# --- فحص مكتبة qrcode ---
try:
    import qrcode
    from io import BytesIO
    QRCODE_AVAILABLE = True
except ImportError:
    QRCODE_AVAILABLE = False

# --- دوال مساعدة ---
# --- فحص مكتبات تحويل Excel ---
try:
    import pandas
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import arabic_reshaper
    from bidi.algorithm import get_display
    ARABIC_SUPPORT_AVAILABLE = True
except ImportError:
    ARABIC_SUPPORT_AVAILABLE = False


try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False

def format_size(size_bytes):
    if size_bytes is None or size_bytes <= 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    # --- إصلاح مشكلة الوحدات الخاطئة للسرعات البطيئة ---
    # إذا كان الحجم أقل من 1024، فهو دائماً بالبايت (B).
    if size_bytes < 1024:
        return f"{int(size_bytes)} B"
    try:
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except (ValueError, OverflowError):
        return "0B"

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

local_bin_path = os.path.join(os.getcwd(), "bin")
if os.path.isdir(local_bin_path):
    os.environ["PATH"] = local_bin_path + os.pathsep + os.environ["PATH"]
FFMPEG_AVAILABLE = shutil.which('ffmpeg') is not None

# --- 1. كائن الإشارات: للتواصل من الخلفية إلى الواجهة ---
# هذا الكائن هو الجسر الآمن بين خيط التحميل وخيط الواجهة الرئيسي.
class WorkerSignals(QObject):
    progress = Signal(int, int, str)    # task_id, percent, status_text
    finished = Signal(int, str)         # task_id, final_message
    error = Signal(int, str)            # task_id, error_message
    cancelled = Signal(int)             # task_id
    paused = Signal(int, str)           # task_id, status_text
    resumed = Signal(int)               # task_id
    details_fetched = Signal(str, str, object) # url, filename, total_size (object for None)
    info_fetched = Signal(str, dict)    # url, info_dict
    playlist_info_fetched = Signal(str, dict) # url, playlist_info_dict
    fetch_error = Signal(str)           # error_message
    playlist_fetched_for_repair = Signal(str, dict) # Special signal for the repair tool
    conversion_finished = Signal(str)   # message
    merge_finished = Signal(str)        # message
    ffmpeg_progress = Signal(str)       # message for ffmpeg install button
    telegram_code_required = Signal()      # Emitted when Telegram asks for a login code
    telegram_password_required = Signal()  # Emitted when Telegram asks for a 2FA password
    telegram_login_finished = Signal(bool, str) # success, message/session_string
    telegram_qr_updated = Signal(QImage)   # Emits a new QR code image
    ffmpeg_finished = Signal(bool, str) # success/fail, final message
    ytdlp_updated = Signal(bool, str)   # success/fail, final message
    # --- إشارات جديدة لمدير تيليجرام ---
    dialogs_fetched = Signal(list)      # list of dialog dicts
    dialog_found = Signal(dict)         # --- جديد: إشارة لكل قناة يتم العثور عليها
    dialog_fetch_error = Signal(str)    # error message
    # --- تعديل: إشارات جديدة لجلب الملفات بشكل تدريجي ---
    channel_left = Signal(int, str)     # dialog_id, message
    file_found = Signal(dict)           # Emitted for each file found
    file_fetch_finished = Signal(int)   # Emitted when fetching is complete (sends total count)
    # --- جديد: إشارات لجلب قائمة تشغيل يوتيوب بشكل تدريجي ---
    yt_video_found = Signal(dict)       # Emitted for each YouTube video entry
    yt_playlist_fetch_finished = Signal(str, int) # Emitted with title and total count
    files_fetched = Signal(list)        # list of file dicts
    # join_finished is no longer needed as the new logic is simpler

# --- عامل جديد لجلب معلومات الرابط فقط ---
class DetailsFetcher(QRunnable):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        """Fetches details in a worker thread and emits a signal."""
        filename, total_size, error_message = get_download_details(self.url)
        if filename:
            self.signals.details_fetched.emit(self.url, filename, total_size)
        else:
            final_error = error_message or "فشل جلب معلومات الرابط المباشر."
            self.signals.fetch_error.emit(final_error)

class YTDLInfoFetcher(QRunnable):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.signals = WorkerSignals()
    @Slot()
    def run(self):
        info, error = get_yt_dlp_info(self.url)
        if info: self.signals.info_fetched.emit(self.url, info)
        else: self.signals.fetch_error.emit(f"خطأ من yt-dlp: {error}")

class YTDLPlaylistFetcher(QRunnable):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.signals = WorkerSignals()

    def _video_callback(self, entry):
        """Callback function to emit a signal for each video found."""
        self.signals.yt_video_found.emit(entry)

    @Slot()
    def run(self):
        try:
            # استدعاء الدالة الجديدة التي تدعم العرض التدريجي
            playlist_title = get_yt_dlp_playlist_entries(self.url, self._video_callback)
            self.signals.yt_playlist_fetch_finished.emit(playlist_title, 0) # Count is not critical here
        except Exception as e:
            self.signals.fetch_error.emit(f"خطأ من yt-dlp: {e}")

class TelethonFetcher(QRunnable):
    """
    Uses Telethon to fetch messages from a Telegram channel, including private ones.
    """
    def __init__(self, url, api_id, api_hash, phone, session_string, for_repair=False):
        super().__init__()
        self.url = url
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.for_repair = for_repair
        self.session_string = session_string
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        client = None # Initialize client to None
        try:
            client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash, loop=loop)

            async def do_fetch():
                await client.connect()

                # Check authorization and raise an exception on failure
                if not await client.is_user_authorized():
                    raise Exception("جلسة Telethon غير صالحة. يرجى تسجيل الدخول من الإعدادات.")

                # Get entity and handle potential errors
                channel_entity = await client.get_entity(self.url)
                
                # Use an async for loop to iterate through messages
                messages = []
                async for msg in client.iter_messages(channel_entity, limit=None): # Fetch all messages
                    if msg.file or msg.photo: # --- تعديل: قبول أي نوع من الملفات أو الصور ---
                        messages.append(msg) # نقبل أي رسالة تحتوي على ملف أو صورة

                if not messages:
                    raise Exception("لم يتم العثور على أي ملفات في القناة.")

                # --- FIX: Build a more robust internal URL ---
                # Use the channel's username if available, as it's a more reliable
                # identifier for get_entity. Fallback to the numeric ID if no username exists.
                def make_entry(msg):
                    # use username if available, otherwise fallback to numeric id
                    channel_ref = getattr(channel_entity, 'username', None) or str(getattr(channel_entity, 'id', ''))
                    # --- تعديل: تحديد اسم الملف بناءً على وجود ملف أو صورة ---
                    media_obj = msg.photo or msg.file
                    filename = getattr(media_obj, 'name', None) or f"telegram_{channel_ref}_{msg.id}.jpg"

                    return {
                        'id': msg.id, # Keep id for potential future use
                        'title': f"[{msg.date.strftime('%Y-%m-%d')}] {filename}",
                        'url': f"telethon://{channel_ref}/{msg.id}", # الرابط الداخلي الخاص بالبرنامج
                        'filename': filename
                    }

                playlist_info = {
                    'title': getattr(channel_entity, 'title', self.url),
                    'entries': [make_entry(msg) for msg in messages]
                }
                if self.for_repair:
                    self.signals.playlist_fetched_for_repair.emit(self.url, playlist_info)
                else:
                    self.signals.playlist_info_fetched.emit(self.url, playlist_info)

            loop.run_until_complete(do_fetch())
        except Exception as e:
            error_message = f"خطأ من Telethon: {e}"
            self.signals.fetch_error.emit(error_message)
        finally:
            # Ensure disconnection and loop closure in all cases
            if client and client.is_connected():
                client.disconnect()
            if loop.is_running(): loop.close()
class TelethonDownloadWorker(QRunnable):

    """
    A dedicated worker to download a specific Telegram message using Telethon.
    """
    def __init__(self, task_id, telethon_url, download_folder, filename, api_id, api_hash, session_string, update_callback=None, environment:str ='desktop'):
        super().__init__()
        self.update_callback = update_callback
        self.signals = WorkerSignals()
        self.task_id = task_id
        self.telethon_url = telethon_url
        self.environment = environment
        self.download_folder = download_folder
        self.filename = filename
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.stop_flag = threading.Event()
        # --- تعديل: استخدام Semaphore للتحكم في عدد الاتصالات المتزامنة ---
        self.pause_flag = threading.Event()
        self.total_size = 0 # سيتم تحديثه لاحقاً
        
        # --- إضافة متغيرات لحساب السرعة ---
        self.last_update_time = time.monotonic()
        self.last_downloaded = 0
        self.smoothed_speed = 0.0
        self.SMOOTHING_FACTOR = 0.2 # نفس معامل التنعيم المستخدم في التحميلات العادية
        
        # --- تعديل: إعادة تفعيل التحميل المقسم للملفات (غير الصور) ---
        self.NUM_SEGMENTS = 4 # إجمالي عدد الأجزاء
        # --- تحسين السرعة: زيادة عدد الأجزاء المتزامنة ---
        # رفع هذه القيمة من 1 إلى 4 يسمح بتحميل 4 أجزاء من الملف في نفس الوقت،
        # مما يزيد من سرعة التحميل بشكل كبير للملفات الكبيرة.
        self.MAX_CONCURRENT_SEGMENTS = 4 # عدد الأجزاء التي تعمل في نفس الوقت
        self.segment_progress = [0] * self.NUM_SEGMENTS
        self.worker_threads = []

    def _send_update(self, data):
        """Helper to send updates via callback (for web) or signals (for PySide)."""
        if self.update_callback:
            # Web UI: Use the provided callback function
            self.update_callback(data)
        else:
            # PySide UI: Emit signals based on the update type
            update_type = data.get("type")
            if update_type == "progress": self.signals.progress.emit(self.task_id, data.get("percent", 0), data.get("text", ""))
            elif update_type == "done": self.signals.finished.emit(self.task_id, data.get("text", ""))
            elif update_type == "error": self.signals.error.emit(self.task_id, data.get("text", ""))
            elif update_type == "paused": self.signals.paused.emit(self.task_id, data.get("text", ""))
            elif update_type == "resumed": self.signals.resumed.emit(self.task_id)
            elif update_type == "canceled": self.signals.cancelled.emit(self.task_id)

    def cancel(self):
        self.stop_flag.set()
        self.pause_flag.set() # Also trigger pause to unblock loops
        
        # --- الإصلاح: إرسال إشارة فورية للواجهة لإزالة العنصر ---
        self._send_update({"task_id": self.task_id, "type": "canceled"})

    def toggle_pause_resume(self):
        if self.pause_flag.is_set():
            self.pause_flag.clear()
        else:
            self.pause_flag.set()

    def _simple_progress_callback(self, current, total):
        self.total_size = total # تخزين الحجم الإجمالي

        if self.stop_flag.is_set():
            # This is a way to interrupt the download in Telethon
            raise Exception("Download cancelled by user.")
        
        current_time = time.monotonic()
        elapsed_time = current_time - self.last_update_time

        # --- تحديث الواجهة كل نصف ثانية لتجنب إرهاقها ---
        if elapsed_time >= 0.5:
            speed = (current - self.last_downloaded) / elapsed_time
            self.smoothed_speed = (speed * self.SMOOTHING_FACTOR) + (self.smoothed_speed * (1 - self.SMOOTHING_FACTOR))
            
            eta = (total - current) / self.smoothed_speed if self.smoothed_speed > 0 else None
            percent = int((current / total) * 100)
            
            text = f"{self.filename} - {format_size(current)} / {format_size(total)} ({percent}%) | {format_size(self.smoothed_speed)}/s | ETA: {format_eta(eta)}"
            self._send_update({"task_id": self.task_id, "type": "progress", "percent": percent, "text": text})

            self.last_downloaded = current
            self.last_update_time = current_time

    @Slot()
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        output_path = os.path.join(self.download_folder, self.filename)
        client = None # Initialize client to None for safety
        state_key = os.path.normpath(output_path)

        try:            
            client = TelegramClient(
                StringSession(self.session_string), self.api_id, self.api_hash, loop=loop
            )

            async def do_download():
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("جلسة Telethon غير صالحة. يرجى تسجيل الدخول مرة أخرى.")

                parts = self.telethon_url.replace("telethon://", "").split('/')
                channel_ref, msg_id = parts[0], int(parts[1])
 
                channel_entity = await client.get_entity(channel_ref)
                message = await client.get_messages(channel_entity, ids=msg_id)
 
                if not message or not message.media:
                    raise Exception("لم يتم العثور على ملف أو صورة في الرسالة.")

                await client.download_media(
                    message.media,
                    file=output_path,
                    progress_callback=self._simple_progress_callback
                )

            loop.run_until_complete(do_download())
            self._send_update({"task_id": self.task_id, "type": "done", "text": f"✅ تم تحميل {self.filename} بنجاح"})
        except Exception as e:
            if not self.stop_flag.is_set():
                self._send_update({"task_id": self.task_id, "type": "error", "text": f"❌ خطأ أثناء تحميل Telethon: {e}"})
        finally:
            if client and client.is_connected():
                client.disconnect()
            if loop.is_running(): loop.close()

# --- 2. العامل (Worker): منطق التحميل الذي يعمل في الخلفية ---
# هذا العامل الجديد هو "الجسر" بين DownloadTask القديم وواجهة PySide6
class CoreDownloadWorker(QRunnable): # --- FIX: Add repair_mode to init ---
    def __init__(self, task_id, url, filepath, total_size, start_paused=False, repair_mode=False):
        super().__init__()
        self.signals = WorkerSignals() # --- FIX: Add repair_mode to init ---
        self.task_id = task_id # --- إصلاح: تخزين رقم تعريف المهمة ---
        self.update_queue = queue.Queue()

        # إنشاء نسخة من "عقل" البرنامج الأصلي
        self.task_logic = DownloadTask(
            task_id=task_id,
            url=url,
            filepath=filepath,
            total_size=total_size,
            update_queue=self.update_queue,
            start_paused=start_paused,
            repair_mode=repair_mode # --- FIX: Pass repair_mode to DownloadTask ---
        )

    # هذه الدوال ستتحكم في منطق التحميل من الواجهة
    def cancel(self):
        # --- إصلاح: إرسال إشارة الإلغاء فوراً للواجهة ---
        # هذا يضمن إزالة عنصر التحميل من الشاشة مباشرة عند الضغط على "إلغاء".
        self.signals.cancelled.emit(self.task_id)
        self.task_logic.cancel() # ثم إيقاف عملية التحميل في الخلفية

    def toggle_pause_resume(self):
        self.task_logic.toggle_pause_resume()

    def pause_for_exit(self):
        """Special pause function for application shutdown."""
        if hasattr(self.task_logic, 'pause_and_exit'):
            self.task_logic.pause_and_exit()

    @Slot()
    def run(self):
        # بدء خيط التحميل الفعلي
        self.task_logic.start()

        # حلقة المراقبة: تقرأ من الـ queue وترسل إشارات للواجهة
        while self.task_logic.is_alive() or not self.update_queue.empty():
            try:
                update = self.update_queue.get(timeout=0.1)
                task_id = update.get("task_id")
                update_type = update.get("type")

                if update_type == "progress":
                    self.signals.progress.emit(task_id, update.get("percent", 0), update.get("text", ""))
                elif update_type == "done":
                    # --- التحقق من حجم الملف النهائي قبل إعلان النجاح ---
                    final_filepath = self.task_logic.filepath
                    expected_size = self.task_logic.total_size

                    # يتم التحقق فقط إذا كان الحجم المتوقع معروفاً (وليس تحميل غير محدد)
                    if expected_size is not None and expected_size > 0:
                        try:
                            actual_size = os.path.getsize(final_filepath)
                            if actual_size != expected_size:
                                error_msg = f"خطأ: حجم الملف ({format_size(actual_size)}) لا يطابق حجم الخادم ({format_size(expected_size)})."
                                self.signals.error.emit(task_id, error_msg)
                                # تنظيف الملف التالف وملف الحالة الخاص به
                                try:
                                    state_file = final_filepath + '.json'
                                    if os.path.exists(final_filepath): os.remove(final_filepath)
                                    if os.path.exists(state_file): os.remove(state_file)
                                except OSError: pass # إذا فشل الحذف، على الأقل تم الإبلاغ عن الخطأ
                                continue # تخطي إرسال إشارة "الانتهاء"
                        except OSError as e:
                            error_msg = f"خطأ: تعذر قراءة حجم الملف النهائي. {e}"
                            self.signals.error.emit(task_id, error_msg)
                            continue # تخطي إرسال إشارة "الانتهاء"
                    
                    # إذا نجح التحقق أو لم يكن مطلوباً، أرسل إشارة النجاح
                    self.signals.finished.emit(task_id, update.get("text", "✅ اكتمل التحميل بنجاح!"))
                elif update_type == "error":
                    self.signals.error.emit(task_id, update.get("text", ""))
                elif update_type == "paused":
                    self.signals.paused.emit(task_id, update.get("text", ""))
                elif update_type == "resumed":
                    self.signals.resumed.emit(task_id)
                elif update_type == "canceled":
                    self.signals.cancelled.emit(task_id)

            except queue.Empty:
                continue

class YTDLDownloadWorker(QRunnable):
    def __init__(self, task_id, url, download_folder, format_id, audio_only=False):
        super().__init__()
        self.signals = WorkerSignals()
        self.task_id = task_id # --- FIX: Store the task_id ---
        self.update_queue = queue.Queue()
        self.task_logic = YTDLRunner(task_id, url, download_folder, self.update_queue, format_id, audio_only)

    def cancel(self):
        # --- إصلاح: إرسال إشارة الإلغاء فوراً للواجهة ---
        # هذا يضمن إزالة عنصر التحميل من الشاشة مباشرة عند الضغط على "إلغاء".
        self.signals.cancelled.emit(self.task_id)
        # --- جديد: استدعاء دالة الإلغاء الجديدة التي تنظف الملفات ---
        self.task_logic.cancel()

    def toggle_pause_resume(self):
        # This simulates pause/resume for yt-dlp
        if self.task_logic.is_paused:
            # If it's paused, signal it to resume
            self.task_logic.resume_flag.set()
            # --- إصلاح: إرسال إشارة "resumed" يدوياً للواجهة ---
            # هذا يضمن تغيير الزر مرة أخرى إلى "إيقاف مؤقت".
            self.signals.resumed.emit(self.task_id)
        else:
            # If it's running, signal it to pause
            self.task_logic.pause_flag.set()
            # --- إصلاح: إرسال إشارة "paused" يدوياً للواجهة ---
            # هذا يضمن تغيير الزر إلى "استئناف" وعرض الحالة الصحيحة.
            # yt-dlp لا يرسل هذه الإشارة بنفسه عند الإيقاف.
            self.signals.paused.emit(self.task_id, "متوقف مؤقتاً")

    @Slot()
    def run(self):
        self.task_logic.start()
        while self.task_logic.is_alive() or not self.update_queue.empty():
            try:
                update = self.update_queue.get(timeout=0.1)
                update_type = update.get("type")
                task_id = update.get("task_id")
                if update_type == "progress": self.signals.progress.emit(task_id, update.get("percent", 0), update.get("text", ""))
                elif update_type == "done": self.signals.finished.emit(task_id, update.get("text", "✅ اكتمل التحميل بنجاح!"))
                elif update_type == "error": self.signals.error.emit(task_id, update.get("text", ""))
                elif update_type == "canceled": self.signals.cancelled.emit(task_id)
            except queue.Empty:
                continue

class ImageConverterWorker(QRunnable):
    def __init__(self, input_path, output_path, output_format):
        super().__init__()
        self.signals = WorkerSignals()
        self.input_path = input_path
        self.output_path = output_path
        self.output_format = output_format

    @Slot()
    def run(self):
        try:
            with Image.open(self.input_path) as img:
                output_format_upper = self.output_format.upper()

                # --- معالجة خاصة ومحسّنة لصيغة ICO ---
                if output_format_upper == 'ICO':
                    # لإنشاء أيقونة عالية الجودة ومتعددة الأحجام، نوفر قائمة بالأحجام القياسية.
                    # مكتبة Pillow تتولى تغيير الحجم لكل طبقة في الأيقونة.
                    icon_sizes = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
                    img.save(self.output_path, format='ICO', sizes=icon_sizes)
                
                # --- معالجة خاصة للصيغ التي لا تدعم الشفافية ---
                elif output_format_upper in ['JPG', 'JPEG', 'BMP']:
                    # إذا كانت الصورة المصدر تحتوي على قناة ألفا (شفافية)، يجب التعامل معها.
                    # الطريقة الأكثر أمانًا هي لصقها على خلفية بلون خالص (مثل الأبيض).
                    if img.mode == 'RGBA' or (img.mode == 'P' and 'transparency' in img.info):
                        # تحويل الصورة إلى RGBA لضمان وجود قناة ألفا للتعامل معها
                        img_rgba = img.convert('RGBA')
                        # إنشاء خلفية بيضاء
                        background = Image.new("RGB", img_rgba.size, (255, 255, 255))
                        # لصق الصورة على الخلفية، باستخدام قناة ألفا كقناع للدمج الصحيح
                        background.paste(img_rgba, mask=img_rgba)
                        background.save(self.output_path, format=output_format_upper)
                    else:
                        # إذا لم تكن هناك شفافية، فقط قم بالتحويل إلى RGB للحفظ
                        img.convert('RGB').save(self.output_path, format=output_format_upper)
                
                # --- لجميع الصيغ الأخرى التي تدعم الشفافية (PNG, WEBP, etc.) ---
                else:
                    # لا حاجة لمعالجة خاصة، Pillow تقوم بالعمل الصحيح.
                    img.save(self.output_path, format=output_format_upper)

            self.signals.conversion_finished.emit(f"✅ تم الحفظ بشكل صحيح في: {os.path.abspath(self.output_path)}")
        except Exception as e:
            self.signals.conversion_finished.emit(f"❌ خطأ أثناء التحويل: {e}")

class ExcelToPdfWorker(QRunnable):
    """
    عامل لتحويل ملف Excel إلى PDF في الخلفية باستخدام pandas و reportlab.
    """
    def __init__(self, input_path, output_path):
        super().__init__()
        self.signals = WorkerSignals()
        self.input_path = input_path
        self.output_path = output_path

    @Slot()
    def run(self):
        try:
            # تسجيل خط يدعم اللغة العربية (Droid Arabic Kufi كمثال)
            # يجب أن يكون ملف الخط موجوداً بجانب ملفات البرنامج
            arabic_font_path = "NotoKufiArabic-Regular"
            arabic_font_path = "NotoKufiArabic-Regular.ttf"
            if os.path.exists(arabic_font_path):
                pdfmetrics.registerFont(TTFont('Arabic-Kufi', arabic_font_path))
                font_name = 'Arabic-Kufi'
                font_name = 'Arabic-Kufi' # --- FIX: Use the registered font name
            else:
                # استخدام خط افتراضي إذا لم يتم العثور على الخط العربي
                font_name = 'Helvetica'

            # قراءة كل الصفحات من ملف Excel
            xls = pandas.ExcelFile(self.input_path)
            doc = SimpleDocTemplate(self.output_path)
            elements = []
            styles = getSampleStyleSheet()
            
            # نمط مخصص يدعم اللغة العربية
            arabic_style = styles['Normal']
            arabic_style.fontName = font_name
            arabic_style.fontSize = 10

            for sheet_name in xls.sheet_names:
                df = pandas.read_excel(xls, sheet_name=sheet_name)
                # --- إصلاح: تجاهل الصفحات الفارغة لمنع حدوث خطأ ---
                # إذا كانت الصفحة لا تحتوي على أعمدة أو صفوف، نتخطاها.
                if df.empty:
                    continue

                # إضافة اسم الصفحة كعنوان
                elements.append(Paragraph(f"الصفحة: {sheet_name}", arabic_style))

                # تحويل البيانات إلى قائمة من القوائم للجدول
                # مع التأكد من أن كل البيانات نصية للتعامل معها بشكل صحيح
                data = [df.columns.tolist()] + df.astype(str).values.tolist()
                # --- FIX: Process data for Arabic shaping and bidi ---
                processed_data = []
                # Process headers
                headers = [get_display(arabic_reshaper.reshape(str(col))) for col in df.columns]
                processed_data.append(headers)

                # Process rows
                for index, row in df.iterrows():
                    processed_row = []
                    for item in row:
                        # Reshape and apply bidi algorithm to each cell
                        processed_row.append(get_display(arabic_reshaper.reshape(str(item))))
                    processed_data.append(processed_row)
                
                # إنشاء الجدول
                table = Table(data, repeatRows=1)
                table = Table(processed_data, repeatRows=1)

                # إضافة نمط للجدول (حدود، ألوان، محاذاة، ودعم العربية)
                style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, -1), font_name), # تطبيق الخط العربي على كل الجدول
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ])
                table.setStyle(style)
                elements.append(table)
                elements.append(PageBreak())

            doc.build(elements)
            self.signals.conversion_finished.emit(f"✅ تم تحويل Excel إلى PDF بنجاح!\nتم الحفظ في: {os.path.abspath(self.output_path)}")
        except Exception as e:
            self.signals.conversion_finished.emit(f"❌ فشل تحويل Excel إلى PDF: {e}")

class VideoMergerWorker(QRunnable):
    def __init__(self, video_path, audio_path, output_path):
        super().__init__()
        self.signals = WorkerSignals()
        self.video_path = video_path
        self.audio_path = audio_path
        self.output_path = output_path

    @Slot()
    def run(self):
        try:
            command = ['ffmpeg', '-i', self.video_path, '-i', self.audio_path, '-c:v', 'copy', '-c:a', 'copy', '-shortest', '-movflags', 'faststart', self.output_path]
            
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

            process = subprocess.run(command, check=True, capture_output=True, text=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')
            
            success_message = f"✅ تم الدمج بنجاح! تم الحفظ في:\n{os.path.abspath(self.output_path)}"
            self.signals.merge_finished.emit(success_message)
        except subprocess.CalledProcessError as e:
            error_message = f"❌ فشل الدمج. تحقق من الملفات المدخلة.\n{e.stderr}"
            self.signals.merge_finished.emit(error_message)
        except Exception as e:
            self.signals.merge_finished.emit(f"❌ حدث خطأ غير متوقع: {e}")

class FileCombinerWorker(QRunnable):
    """A worker to manually combine .part files."""
    def __init__(self, first_part_path):
        super().__init__()
        self.signals = WorkerSignals()
        self.first_part_path = first_part_path

    @Slot()
    def run(self):
        try:
            base_path = self.first_part_path.rsplit('.part', 1)[0]
            output_path = base_path # The final file will have the base name
            with open(output_path, 'wb') as final_file:
                i = 0
                while True:
                    part_file = f"{base_path}.part{i}"
                    if not os.path.exists(part_file): break
                    with open(part_file, 'rb') as pf: shutil.copyfileobj(pf, final_file)
                    i += 1
            self.signals.merge_finished.emit(f"✅ تم دمج {i} أجزاء بنجاح!\nتم الحفظ في: {os.path.abspath(output_path)}")
        except Exception as e:
            self.signals.merge_finished.emit(f"❌ فشل الدمج اليدوي: {e}")

class FileRepairWorker(QRunnable):
    def __init__(self, input_path, output_path, deep_repair=False):
        super().__init__()
        # --- FIX: Reuse progress signal for repair progress ---
        self.signals = WorkerSignals()
        self.input_path = input_path
        self.output_path = output_path
        self.deep_repair = deep_repair

    @Slot()
    def run(self):
        try:
            # --- FIX: Get duration of the input file first ---
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

            # --- Main ffmpeg command ---
            if self.deep_repair:
                # Deep repair (re-encoding). Slower but more effective.
                # Using common codecs (h264/aac) and a fast preset.
                command = [
                    'ffmpeg', '-y', '-i', self.input_path,
                    '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
                    '-c:a', 'aac', '-b:a', '192k',
                    self.output_path
                ]
                success_message = f"✅ تم الإصلاح العميق بنجاح! تم الحفظ في:\n{os.path.abspath(self.output_path)}"
            else:
                # Fast repair (remuxing). Fixes container issues.
                command = ['ffmpeg', '-y', '-i', self.input_path, '-c', 'copy', self.output_path]
                success_message = f"✅ تم الإصلاح السريع بنجاح! تم الحفظ في:\n{os.path.abspath(self.output_path)}"
            
            # --- FIX: Use Popen to read stderr in real-time for progress ---
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')

            for line in process.stdout:
                if "time=" in line and total_duration_sec > 0:
                    time_str = re.search(r"time=(\d{2}):(\d{2}):(\d{2})\.\d+", line)
                    if time_str:
                        h, m, s = map(int, time_str.groups())
                        current_sec = h * 3600 + m * 60 + s
                        percent = int((current_sec / total_duration_sec) * 100)
                        
                        # Emit progress signal (reusing the download progress signal)
                        # We use a special task_id of -2 for the repair tool
                        progress_text = f"جاري الإصلاح... {percent}%"
                        self.signals.progress.emit(-2, percent, progress_text)
            
            process.wait() # Wait for the process to complete

            if process.returncode != 0:
                # If ffmpeg exited with an error
                # We can try to read the full output for the error message
                output, error = process.communicate()
                error_message = f"❌ فشل الإصلاح. قد يكون الملف تالفاً بشدة أو غير مدعوم.\n{error or output}"
                self.signals.merge_finished.emit(error_message)
                return
            
            self.signals.merge_finished.emit(success_message) # Reuse merge_finished signal

        except Exception as e:
            self.signals.merge_finished.emit(f"❌ حدث خطأ غير متوقع: {e}")

class FileCompletionWorker(QRunnable):
    def __init__(self, filepath, url):
        super().__init__()
        self.signals = WorkerSignals() # Reusing signals
        self.filepath = filepath
        self.url = url
        self.stop_flag = threading.Event()
        self.last_update_time = 0

    def cancel(self):
        self.stop_flag.set()

    @Slot()
    def run(self):
        try:
            local_size = os.path.getsize(self.filepath)
            
            # Step 1: Get total size from server
            self.signals.merge_finished.emit(f"جاري الاتصال بالخادم للحصول على معلومات الملف...")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            # Use HEAD request to be more efficient
            with requests.head(self.url, allow_redirects=True, timeout=15, headers=headers, verify=False) as r:
                r.raise_for_status()
                content_length = r.headers.get('content-length')
                if not content_length:
                    self.signals.merge_finished.emit("❌ الخادم لم يرسل حجم الملف. لا يمكن استكمال التحميل.")
                    return
                total_size = int(content_length)

            if local_size >= total_size:
                self.signals.merge_finished.emit(f"✅ الملف المحلي ({format_size(local_size)}) مكتمل أو أكبر من الملف على الخادم ({format_size(total_size)}). لا حاجة للاستكمال.")
                return

            # Step 2: Download the remaining part
            remaining_size = total_size - local_size
            self.signals.merge_finished.emit(f"حجم الملف المحلي: {format_size(local_size)}. سيتم تحميل {format_size(remaining_size)}.")
            time.sleep(1) # Small delay to show the message
            
            range_header = {'Range': f'bytes={local_size}-'}
            range_header.update(headers)

            # --- FIX: Use Popen to show progress ---
            self.signals.progress.emit(-2, 0, "بدء استكمال التحميل...")

            with requests.get(self.url, stream=True, headers=range_header, timeout=60, verify=False) as r:
                r.raise_for_status()
                with open(self.filepath, 'ab') as f:
                    downloaded_chunk_bytes = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        if self.stop_flag.is_set():
                            self.signals.merge_finished.emit("تم إيقاف الاستكمال.")
                            return
                        if chunk:
                            f.write(chunk)
                            downloaded_chunk_bytes += len(chunk)
                            current_time = time.time()
                            # Update progress bar every 0.5 seconds
                            if current_time - self.last_update_time > 0.5:
                                percent = int((downloaded_chunk_bytes / remaining_size) * 100) if remaining_size > 0 else 100
                                progress_text = f"جاري استكمال التحميل... {percent}% ({format_size(downloaded_chunk_bytes)} / {format_size(remaining_size)})"
                                self.signals.progress.emit(-2, percent, progress_text)
                                self.last_update_time = current_time
            
            self.signals.progress.emit(-2, 100, f"✅ تم استكمال الملف بنجاح! الحجم الجديد: {format_size(total_size)}")
            self.signals.merge_finished.emit(f"✅ تم استكمال الملف بنجاح! الحجم الجديد: {format_size(total_size)}") # Final message
        except requests.exceptions.RequestException as e:
            self.signals.merge_finished.emit(f"❌ فشل الاتصال بالرابط: {e}")

class FileCompletionWorker(QRunnable):
    def __init__(self, filepath, url):
        super().__init__()
        self.signals = WorkerSignals() # Reusing signals
        self.filepath = filepath
        self.url = url
        self.stop_flag = threading.Event()
        self.last_update_time = 0

    def cancel(self):
        self.stop_flag.set()

    @Slot()
    def run(self):
        try:
            local_size = os.path.getsize(self.filepath)
            
            # Step 1: Get total size from server
            self.signals.merge_finished.emit(f"جاري الاتصال بالخادم للحصول على معلومات الملف...")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            # Use HEAD request to be more efficient
            with requests.head(self.url, allow_redirects=True, timeout=15, headers=headers, verify=False) as r:
                r.raise_for_status()
                content_length = r.headers.get('content-length')
                if not content_length:
                    self.signals.merge_finished.emit("❌ الخادم لم يرسل حجم الملف. لا يمكن استكمال التحميل.")
                    return
                total_size = int(content_length)

            if local_size >= total_size:
                self.signals.merge_finished.emit(f"✅ الملف المحلي ({format_size(local_size)}) مكتمل أو أكبر من الملف على الخادم ({format_size(total_size)}). لا حاجة للاستكمال.")
                return

            # Step 2: Download the remaining part
            remaining_size = total_size - local_size
            self.signals.merge_finished.emit(f"حجم الملف المحلي: {format_size(local_size)}. سيتم تحميل {format_size(remaining_size)}.")
            time.sleep(1) # Small delay to show the message

            range_header = {'Range': f'bytes={local_size}-'}
            range_header.update(headers)

            # --- FIX: Use Popen to show progress ---
            self.signals.progress.emit(-2, 0, "بدء استكمال التحميل...")

            with requests.get(self.url, stream=True, headers=range_header, timeout=60, verify=False) as r:
                r.raise_for_status()
                with open(self.filepath, 'ab') as f:
                    downloaded_chunk_bytes = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        if self.stop_flag.is_set():
                            self.signals.merge_finished.emit("تم إيقاف الاستكمال.")
                            return
                        if chunk: f.write(chunk)
                        downloaded_chunk_bytes += len(chunk)
                        current_time = time.time()
                        if current_time - self.last_update_time > 0.5:
                            percent = int((downloaded_chunk_bytes / remaining_size) * 100) if remaining_size > 0 else 100
                            progress_text = f"جاري استكمال التحميل... {percent}% ({format_size(downloaded_chunk_bytes)} / {format_size(remaining_size)})"
                            self.signals.progress.emit(-2, percent, progress_text)
                            self.last_update_time = current_time
            self.signals.progress.emit(-2, 100, f"✅ تم استكمال الملف بنجاح! الحجم الجديد: {format_size(total_size)}")
            self.signals.merge_finished.emit(f"✅ تم استكمال الملف بنجاح! الحجم الجديد: {format_size(total_size)}") # Final message
        except requests.exceptions.RequestException as e:
            self.signals.merge_finished.emit(f"❌ فشل الاتصال بالرابط: {e}")

class FfmpegInstallerWorker(QRunnable):
    """Downloads and extracts ffmpeg."""
    def __init__(self):
        super().__init__()
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        FFMPEG_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
        zip_path = "ffmpeg-essentials.zip"
        bin_dir = os.path.join(os.getcwd(), "bin")

        try:
            # 1. Download
            self.signals.ffmpeg_progress.emit('جاري تحميل ffmpeg...')
            with requests.get(FFMPEG_URL, stream=True, timeout=30, verify=False) as r:
                r.raise_for_status()
                total_length = int(r.headers.get('content-length', 0))
                with open(zip_path, 'wb') as f:
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        if total_length > 0:
                            downloaded += len(chunk)
                            percent = int(100 * downloaded / total_length)
                            self.signals.ffmpeg_progress.emit(f'جاري تحميل ffmpeg... {percent}%')

            # 2. Extract
            self.signals.ffmpeg_progress.emit('جاري فك الضغط...')
            os.makedirs(bin_dir, exist_ok=True)
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.infolist():
                    if '/bin/' in member.filename and member.filename.endswith('.exe'):
                        filename = os.path.basename(member.filename)
                        target_path = os.path.join(bin_dir, filename)
                        with zf.open(member) as source, open(target_path, "wb") as target:
                            shutil.copyfileobj(source, target)
            
            # 3. Cleanup and signal success
            os.remove(zip_path)
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ["PATH"]
            self.signals.ffmpeg_finished.emit(True, '✅ تم تثبيت ffmpeg بنجاح!')

        except Exception as e:
            print(f"FFmpeg installation failed: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            self.signals.ffmpeg_finished.emit(False, f'❌ فشل التثبيت. اضغط للمحاولة مرة أخرى.')

class YtdlpUpdaterWorker(QRunnable):
    """Worker to update the yt-dlp library using pip."""
    def __init__(self):
        super().__init__()
        self.signals = WorkerSignals()

    @Slot()
    def run(self):
        try:
            self.signals.ytdlp_updated.emit(False, "جاري تحديث yt-dlp...")
            
            # Command to update yt-dlp using pip
            command = [sys.executable, '-m', 'pip', 'install', '--upgrade', 'yt-dlp']
            
            startupinfo = None
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

            # Run the command
            result = subprocess.run(command, check=True, capture_output=True, text=True, startupinfo=startupinfo, encoding='utf-8', errors='ignore')
            
            # Check the output to see if it was actually updated
            if "Requirement already satisfied" in result.stdout:
                self.signals.ytdlp_updated.emit(True, "✅ مكتبة yt-dlp محدثة بالفعل لآخر إصدار.")
            else:
                self.signals.ytdlp_updated.emit(True, "✅ تم تحديث yt-dlp بنجاح! يرجى إعادة تشغيل البرنامج.")

        except Exception as e:
            error_message = f"❌ فشل تحديث yt-dlp. تأكد من أن Python و pip مثبتان بشكل صحيح.\nالخطأ: {e}"
            self.signals.ytdlp_updated.emit(False, error_message)

class NetworkMonitor(QRunnable):
    """
    Monitors internet connectivity in the background and emits signals on status change.
    Uses a special task_id of -1 to differentiate its signals.
    """
    def __init__(self):
        super().__init__()
        # We can reuse WorkerSignals for simplicity.
        # error -> connection_lost
        # finished -> connection_restored
        self.signals = WorkerSignals()
        self.is_running = True
        # Assume connection is available at start to only trigger on the first loss.
        self.was_connected = True

    @Slot()
    def run(self):
        """Continuously checks for a connection and emits signals on change."""
        while self.is_running:
            try:
                # Ping a reliable server with a short timeout.
                # Using HEAD request is more lightweight than GET.
                requests.head("https://www.google.com", timeout=5, verify=False)
                is_currently_connected = True
            except requests.RequestException:
                is_currently_connected = False

            if self.was_connected and not is_currently_connected:
                self.signals.error.emit(-1, "CONNECTION_LOST")
                self.was_connected = False
            elif not self.was_connected and is_currently_connected:
                self.signals.finished.emit(-1, "CONNECTION_RESTORED")
                self.was_connected = True

            time.sleep(10) # Wait before the next check.

    def stop(self):
        """Signals the monitor to stop its loop."""
        self.is_running = False

# --- 3. واجهة عنصر التحميل الواحد ---
class DownloadTaskWidget(QFrame):
    play_requested = Signal(str)

    def __init__(self, filename, task_id, worker):
        super().__init__()
        self.task_id = task_id
        self.worker = worker
        self.setFrameShape(QFrame.StyledPanel)
        self.is_complete = False
        self.final_filepath = None # لتخزين المسار النهائي
        self.auto_paused_by_network = False # Flag for auto-resume

        layout = QVBoxLayout(self)

        self.filename_label = QLabel(f"{filename}")
        self.status_label = QLabel("في انتظار البدء...")
        self.progress_bar = QProgressBar()
        
        # --- أزرار التحكم ---
        button_layout = QHBoxLayout()
        self.toggle_button = QPushButton("⏸️ إيقاف مؤقت")
        self.cancel_button = QPushButton("إلغاء")
        self.play_button = QPushButton("▶️ تشغيل")
        self.open_folder_button = QPushButton("📂")
        self.open_folder_button.setToolTip("فتح مجلد الملف")
        self.retry_button = QPushButton("🔄")
        self.retry_button.setToolTip("إعادة المحاولة")
        self.delete_log_button = QPushButton("🗑️")
        self.delete_log_button.setToolTip("حذف من السجل")
        self.play_button.setStyleSheet("background-color: #3498db;")
        self.play_button.hide()
        self.retry_button.hide()
        self.delete_log_button.hide()
        # --- إصلاح: التحقق من وجود دالة cancel قبل ربطها ---
        # هذا يمنع حدوث خطأ عند استخدام عامل وهمي في سجل التحميلات.
        if hasattr(self.worker, 'cancel'):
            self.cancel_button.clicked.connect(self.worker.cancel)
        self.play_button.clicked.connect(self._request_play)
        
        self.open_folder_button.clicked.connect(self._open_containing_folder)
        self.open_folder_button.hide() # إخفاء الزر في البداية
        
        button_layout.addStretch()
        button_layout.addWidget(self.toggle_button)
        button_layout.addWidget(self.cancel_button)

        layout.addWidget(self.filename_label)
        layout.addWidget(self.status_label)
        layout.addWidget(self.progress_bar)
        layout.addLayout(button_layout)
        self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #2ecc71; }")

        # --- الجزء السفلي للأزرار بعد الاكتمال ---
        self.completed_buttons_layout = QHBoxLayout()
        self.completed_buttons_layout.addStretch()
        self.completed_buttons_layout.addWidget(self.retry_button)
        self.completed_buttons_layout.addWidget(self.play_button)
        self.completed_buttons_layout.addWidget(self.open_folder_button)
        self.completed_buttons_layout.addWidget(self.delete_log_button)
        layout.addLayout(self.completed_buttons_layout)

        self.toggle_button.clicked.connect(self._manual_toggle)

    def _manual_toggle(self):
        """Handles a manual click from the user, ensuring auto-resume is disabled."""
        self.auto_paused_by_network = False
        self.worker.toggle_pause_resume()

    def _request_play(self):
        filepath = ""
        self.play_requested.emit(self.final_filepath)

    def _get_final_filepath(self):
        """Helper to get the final file path from any worker type."""
        if hasattr(self.worker, 'task_logic') and hasattr(self.worker.task_logic, 'filepath'):
            return os.path.normpath(self.worker.task_logic.filepath)
        elif hasattr(self.worker, 'filename') and hasattr(self.worker, 'download_folder'):
            return os.path.normpath(os.path.join(self.worker.download_folder, self.worker.filename))
        elif self.final_filepath:
            return self.final_filepath
        return None

    def _open_containing_folder(self):
        # --- إصلاح: استخدام المسار الصحيح من السجل ---
        # في سجل التحميلات، المسار الصحيح مخزن في self.final_filepath
        # الذي يتم تعيينه بواسطة دالة setup_for_log.
        filepath = self.final_filepath

        if filepath and os.path.exists(filepath):
            if sys.platform == "win32":
                subprocess.Popen(f'explorer /select,"{filepath}"')
            elif sys.platform == "darwin": # macOS
                subprocess.Popen(["open", "-R", filepath])
            else: # Linux
                subprocess.Popen(["xdg-open", os.path.dirname(filepath)])

    @Slot(int, str)
    def update_progress(self, percent, status_text):
        if percent < 0:
            # A negative value indicates indeterminate progress
            if self.progress_bar.maximum() != 0:
                self.progress_bar.setRange(0, 0)
        else:
            # Set back to determinate mode if it was indeterminate
            if self.progress_bar.maximum() == 0:
                self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(percent)
        self.status_label.setText(status_text)

    @Slot(str)
    def set_finished(self, message):
        self.final_filepath = self._get_final_filepath()
        self.progress_bar.setValue(100)
        self.status_label.setText(message)
        self.toggle_button.hide()
        self.cancel_button.hide()
        self.is_complete = True

        VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv']
        if self.final_filepath:
            _, ext = os.path.splitext(self.final_filepath)
            if ext.lower() in VIDEO_EXTENSIONS: self.play_button.show()
        self.open_folder_button.show()

    @Slot(str)
    def set_error(self, message):
        self.final_filepath = self._get_final_filepath()
        self.status_label.setText(message)
        self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: red; }")
        self.toggle_button.hide()
        self.cancel_button.hide()
        self.retry_button.show()
        self.open_folder_button.show() # Show folder even on error to check partial files
        self.is_complete = True

    def setup_for_log(self, log_entry):
        """Configure the widget to be displayed in the log dialog."""
        self.is_complete = True
        self.final_filepath = log_entry.get("filepath")
        self.filename_label.setText(log_entry.get("filename", "اسم غير معروف"))
        
        final_size_str = format_size(log_entry.get("final_size", 0))
        status_text = f"{log_entry.get('status_text')} ({final_size_str})"

        if log_entry.get("status") == "finished":
            self.set_finished(status_text)
        else: # error
            self.set_error(status_text)
            self.retry_button.show()

        self.delete_log_button.show()
        self.progress_bar.hide() # No need for progress bar in log

    @Slot(str)
    def set_paused(self, message):
        self.status_label.setText(message)
        self.toggle_button.setText("▶️ استئناف")
        self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #f1c40f; }")

    def set_resumed(self):
        self.toggle_button.setText("⏸️ إيقاف مؤقت")
        self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #2ecc71; }")

# --- 4. نافذة اختيار الجودة ---
class QualitySelectionDialog(QDialog):
    format_selected = Signal(str)

    def __init__(self, formats, parent=None):
        super().__init__(parent)
        self.setWindowTitle("اختر الجودة المطلوبة")
        self.setMinimumSize(600, 400)
        self.setStyleSheet(parent.styleSheet())

        self.format_map = {}
        layout = QVBoxLayout(self)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["الجودة", "النوع", "الحجم"])
        self.tree.header().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.tree)
        
        for f in formats:
            filesize_str = format_size(f['filesize']) if f['filesize'] else "N/A"
            
            # --- التعديل الأساسي: تحويل الدقة إلى الصيغة المطلوبة (e.g., 720p) ---
            resolution = f.get('resolution', 'N/A')
            height = 0
            if 'x' in resolution:
                try:
                    height = int(resolution.split('x')[1])
                    resolution = f"{height}p"
                except (ValueError, IndexError):
                    pass # Keep original resolution string if parsing fails
            
            # --- تحسين: تمييز الجودات العالية (HD, FHD, 2K, 4K) بشكل دقيق ---
            if height >= 4320:
                resolution = f"{resolution} (8K)"
            elif height >= 2160:
                resolution = f"{resolution} (4K)"
            elif height >= 1440:
                resolution = f"{resolution} (2K)"
            elif height >= 1080:
                resolution = f"{resolution} (FHD)"
            elif height >= 720:
                resolution = f"{resolution} (HD)"
            
            item = QTreeWidgetItem([resolution, f['ext'], filesize_str])
            self.tree.addTopLevelItem(item)
            self.format_map[id(item)] = f['id']

        self.tree.doubleClicked.connect(self.on_download)

        button_layout = QHBoxLayout()
        download_btn = QPushButton("⬇️ تحميل المحدد")
        cancel_btn = QPushButton("إلغاء")
        button_layout.addWidget(download_btn)
        button_layout.addWidget(cancel_btn)
        layout.addLayout(button_layout)

        download_btn.clicked.connect(self.on_download)
        cancel_btn.clicked.connect(self.reject)

    def on_download(self):
        selected_items = self.tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "لم يتم التحديد", "يرجى اختيار جودة من القائمة.")
            return
        
        selected_item = selected_items[0]
        format_id = self.format_map.get(id(selected_item))
        if format_id:
            self.format_selected.emit(format_id)
            self.accept()

    @staticmethod
    def _filter_and_sort_formats(formats):
        filtered = []
        for f in formats:
            # --- تجاهل الصيغ التي لا تحتوي على فيديو ---
            if f.get('vcodec') == 'none' or not f.get('resolution'):
                continue
            
            filtered.append({
                'id': f['format_id'], 'resolution': f.get('resolution') or 'N/A', 
                'ext': f.get('ext'), 'fps': f.get('fps'), 
                'filesize': f.get('filesize') or f.get('filesize_approx')
            })

        def sort_key(item):
            res_str = item['resolution']
            return int(res_str.split('x')[1]) if 'x' in res_str else 0
        filtered.sort(key=sort_key, reverse=True)
        return filtered

class PlaylistSelectionDialog(QDialog):
    """A dialog to display playlist entries (e.g., from a Telegram channel) and select one to download."""
    # --- تعديل: إرسال قائمة من الروابط بدلاً من رابط واحد ---
    videos_selected = Signal(list) # Emits a list of URLs of the selected videos

    def __init__(self, playlist_info=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("جاري جلب الفيديوهات...")
        self.setMinimumSize(800, 600) # زيادة حجم النافذة قليلاً
        self.setStyleSheet(parent.styleSheet())

        self.entries = []
        layout = QVBoxLayout(self)

        # --- تعديل: إضافة مربع اختيار "تحديد الكل" في الأعلى ---
        self.select_all_checkbox = QCheckBox("تحديد الكل / إلغاء تحديد الكل")
        self.select_all_checkbox.stateChanged.connect(self.toggle_all_items)
        layout.addWidget(self.select_all_checkbox)

        # --- إضافة: شريط بحث لفلترة النتائج ---
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍 ابحث عن اسم ملف...")
        self.search_input.textChanged.connect(self.filter_items)
        layout.addWidget(self.search_input)


        self.list_widget = QListWidget()
        # --- تعديل: استخدام مربعات الاختيار بدلاً من التحديد العادي ---
        # لا نحتاج إلى setSelectionMode بعد الآن

        # --- إصلاح: ربط حدث النقر على العنصر لتغيير حالة مربع الاختيار ---
        self.list_widget.itemClicked.connect(self.toggle_item_checkstate)

        if playlist_info:
            self.setWindowTitle(f"اختر فيديو من: {playlist_info.get('title', 'قناة')}")
            self.add_multiple_entries(playlist_info.get('entries', []))
        layout.addWidget(self.list_widget)
        # --- تعديل: النقر المزدوج لم يعد يقوم بالتحميل ---

        self.list_widget.itemDoubleClicked.connect(self.on_download)

        # --- تعديل: إزالة أزرار تحديد الكل/إلغاء التحديد القديمة ---
        button_layout = QHBoxLayout()
        download_btn = QPushButton("⬇️ تحميل المحدد (يمكنك تحديد أكثر من ملف)")
        cancel_btn = QPushButton("إلغاء")

        button_layout.addStretch() # إضافة مسافة مرنة
        button_layout.addWidget(download_btn)
        button_layout.addWidget(cancel_btn)
        layout.addLayout(button_layout)

        download_btn.clicked.connect(self.on_download)
        cancel_btn.clicked.connect(self.reject)

    def add_video_entry(self, entry):
        """Adds a single YouTube video entry to the list."""
        if not entry: return
        self.entries.append(entry)
        title = entry.get('title', entry.get('url', 'فيديو غير معروف'))
        if title != '[Private video]':
            item = QListWidgetItem(title, self.list_widget)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            # تخزين الرابط الكامل في بيانات العنصر
            item.setData(Qt.UserRole, entry.get('url'))

    def add_multiple_entries(self, entries):
        """Adds a list of entries (e.g., from Telegram) all at once."""
        self.entries.extend(entries)
        for entry in reversed(entries):
            title = entry.get('title', entry.get('url'))
            if title != '[Private video]':
                item = QListWidgetItem(title, self.list_widget)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(Qt.Unchecked)

    def set_title_and_finish(self, title):
        self.setWindowTitle(title)

    def on_download(self):
        # --- تعديل: تجميع الروابط من العناصر المحددة بمربع الاختيار ---
        selected_urls = []
        has_selection = False
        # نمر على العناصر المحددة بنفس الترتيب الذي تظهر به في القائمة
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                has_selection = True
                # --- تعديل: الحصول على الرابط مباشرة من بيانات العنصر ---
                url_from_data = item.data(Qt.UserRole)
                if url_from_data:
                    selected_urls.append(url_from_data)
                else: # Fallback for older logic (Telegram)
                    selected_title = item.text()
                    selected_entry = next((e for e in self.entries if e.get('title', e.get('url')) == selected_title), None)
                    if selected_entry:
                        selected_urls.append(selected_entry['url'])
        
        if selected_urls:
            self.videos_selected.emit(selected_urls)
            self.accept()

    def toggle_all_items(self, state):
        """
        دالة لتحديد أو إلغاء تحديد كل العناصر بناءً على حالة مربع الاختيار العلوي.
        'state' هو الحالة الجديدة لمربع الاختيار (2 يعني Checked, 0 يعني Unchecked).
        """
        # --- إصلاح: استخدام الحالة الصحيحة لتحديد/إلغاء تحديد العناصر ---
        check_state = Qt.Checked if state == Qt.CheckState.Checked.value else Qt.Unchecked
        for i in range(self.list_widget.count()):
            # منع إرسال إشارات عند تغيير حالة كل عنصر لتجنب الحمل الزائد
            self.list_widget.item(i).setCheckState(check_state)

    def toggle_item_checkstate(self, item):
        """
        دالة لتبديل حالة مربع الاختيار عند النقر على العنصر نفسه.
        """
        current_state = item.checkState()
        item.setCheckState(Qt.Unchecked if current_state == Qt.Checked else Qt.Checked)
        # --- إصلاح: تحديث حالة مربع "تحديد الكل" بناءً على التحديدات الفردية ---
        all_checked = all(self.list_widget.item(i).checkState() == Qt.Checked for i in range(self.list_widget.count()))
        self.select_all_checkbox.blockSignals(True)
        self.select_all_checkbox.setCheckState(Qt.Checked if all_checked else Qt.Unchecked)
        self.select_all_checkbox.blockSignals(False)

    def filter_items(self, text):
        """
        دالة لإظهار/إخفاء العناصر في القائمة بناءً على نص البحث.
        """
        search_text = text.lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            # إخفاء العنصر إذا كان نص البحث غير موجود في عنوانه
            item.setHidden(search_text not in item.text().lower())

class CancelSelectionDialog(QDialog):
    """A dialog to select which tasks to EXCLUDE from cancellation."""
    def __init__(self, tasks, parent=None):
        super().__init__(parent)
        self.setWindowTitle("إلغاء الكل باستثناء...")
        self.setMinimumSize(600, 400)
        self.setStyleSheet(parent.styleSheet())

        self.tasks_to_keep = [] # Store the task_ids to keep
        self.all_tasks = tasks # {task_id: widget}

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("حدد التحميلات التي تريد الإبقاء عليها (لن يتم إلغاؤها):"))

        self.list_widget = QListWidget()
        for task_id, widget in self.all_tasks.items():
            # Create a checkable item for each task
            item_text = widget.filename_label.text()
            item = QListWidgetItem(item_text, self.list_widget)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked) # Default to not keeping the task
            item.setData(Qt.UserRole, task_id) # Store task_id in the item

        layout.addWidget(self.list_widget)

        button_layout = QHBoxLayout()
        confirm_btn = QPushButton("✅ تأكيد الإلغاء")
        confirm_btn.setStyleSheet("background-color: #da373c;") # Red color for confirmation
        cancel_btn = QPushButton("إلغاء")

        button_layout.addStretch()
        button_layout.addWidget(confirm_btn)
        button_layout.addWidget(cancel_btn)
        layout.addLayout(button_layout)

        confirm_btn.clicked.connect(self.on_confirm)
        cancel_btn.clicked.connect(self.reject)

    def on_confirm(self):
        """Collects the task_ids of the checked items and accepts the dialog."""
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                task_id = item.data(Qt.UserRole)
                self.tasks_to_keep.append(task_id)
        self.accept()

    def get_tasks_to_cancel(self):
        """Returns a list of task_ids that were NOT selected to be kept."""
        all_task_ids = set(self.all_tasks.keys())
        tasks_to_keep_ids = set(self.tasks_to_keep)
        return list(all_task_ids - tasks_to_keep_ids)


class VideoLibraryDialog(QDialog):
    play_video_requested = Signal(str)

    def __init__(self, download_folder, parent=None):
        super().__init__(parent)
        self.setWindowTitle("مكتبة الفيديوهات")
        self.setMinimumSize(800, 500)
        self.setStyleSheet(parent.styleSheet())
        self.download_folder = download_folder
        self.video_map = {} # Maps tree item ID to full path

        layout = QVBoxLayout(self)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["اسم الملف", "المجلد", "الحجم"])
        self.tree.header().setSectionResizeMode(QHeaderView.Stretch)
        self.tree.itemDoubleClicked.connect(self._play_selected)
        layout.addWidget(self.tree)

        # Buttons
        button_layout = QHBoxLayout()
        refresh_btn = QPushButton("🔄 تحديث")
        play_btn = QPushButton("▶️ تشغيل المحدد")
        open_folder_btn = QPushButton("📂 فتح المجلد")
        
        refresh_btn.clicked.connect(self.populate_videos)
        play_btn.clicked.connect(self._play_selected)
        open_folder_btn.clicked.connect(self._open_folder_for_selected)

        button_layout.addWidget(refresh_btn)
        button_layout.addStretch()
        button_layout.addWidget(play_btn)
        button_layout.addWidget(open_folder_btn)
        layout.addLayout(button_layout)

        self.populate_videos()

    def populate_videos(self):
        self.tree.clear()
        self.video_map.clear()
        VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv']

        for root, _, files in os.walk(self.download_folder):
            for filename in files:
                if any(filename.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                    full_path = os.path.join(root, filename)
                    try:
                        file_size = os.path.getsize(full_path)
                        relative_folder = os.path.relpath(root, self.download_folder)
                        if relative_folder == ".":
                            relative_folder = "الرئيسي"
                        
                        item = QTreeWidgetItem([filename, relative_folder, format_size(file_size)])
                        self.tree.addTopLevelItem(item)
                        self.video_map[id(item)] = full_path
                    except OSError:
                        continue

    def _get_selected_path(self):
        selected_items = self.tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "لم يتم التحديد", "يرجى اختيار فيديو من القائمة.")
            return None
        return self.video_map.get(id(selected_items[0]))

    def _play_selected(self, event=None):
        path = self._get_selected_path()
        if path:
            self.play_video_requested.emit(path)
            self.accept() # Close the library after playing

    def _open_folder_for_selected(self):
        path = self._get_selected_path()
        if path:
            if sys.platform == "win32":
                subprocess.Popen(f'explorer /select,"{os.path.normpath(path)}"')
            elif sys.platform == "darwin":
                subprocess.Popen(["open", "-R", path])
            else:
                subprocess.Popen(["xdg-open", os.path.dirname(path)])

class DownloadLogDialog(QDialog):
    """A dialog to display and manage the log of completed downloads."""
    log_cleared = Signal()
    retry_requested = Signal(dict)
    delete_entry_requested = Signal(dict)

    def __init__(self, log_entries, parent=None):
        super().__init__(parent)
        self.setWindowTitle("سجل التحميلات")
        self.setMinimumSize(700, 500)
        self.setStyleSheet(parent.styleSheet())
        self.log_entries = log_entries

        layout = QVBoxLayout(self)

        # --- منطقة عرض السجل (قابلة للتمرير) ---
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content_widget = QWidget()
        self.log_layout = QVBoxLayout(scroll_content_widget)
        self.log_layout.setAlignment(Qt.AlignTop)
        scroll_area.setWidget(scroll_content_widget)
        layout.addWidget(scroll_area)

        self.populate_log()

        button_layout = QHBoxLayout()
        clear_btn = QPushButton("🗑️ مسح السجل")
        clear_btn.setStyleSheet("background-color: #c0392b;")
        close_btn = QPushButton("إغلاق")

        button_layout.addWidget(clear_btn)
        button_layout.addStretch()
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)

        clear_btn.clicked.connect(self.on_clear)
        close_btn.clicked.connect(self.accept)

    def populate_log(self):
        # Add items in reverse so the newest entries appear at the top
        for entry in reversed(self.log_entries):
            # We create a dummy worker because the widget expects one, but we won't use it.
            dummy_worker = QObject()
            widget = DownloadTaskWidget(entry.get("filename"), -1, dummy_worker)
            widget.setup_for_log(entry)
            widget.play_requested.connect(self.parent().launch_player)
            widget.retry_button.clicked.connect(lambda checked=False, e=entry: self.retry_requested.emit(e))
            # --- إصلاح: ربط زر "فتح المجلد" بالدالة الصحيحة ---
            widget.open_folder_button.clicked.connect(widget._open_containing_folder)
            widget.delete_log_button.clicked.connect(lambda checked=False, e=entry: self._delete_entry(widget, e))
            self.log_layout.addWidget(widget)

    def on_clear(self):
        while self.log_layout.count():
            child = self.log_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        self.log_cleared.emit()
        QMessageBox.information(self, "تم المسح", "تم مسح السجل بنجاح.")

    def _delete_entry(self, widget, entry):
        # --- إصلاح: التحقق من أن العنصر لا يزال موجوداً قبل التعامل معه ---
        # هذا يمنع حدوث خطأ "already deleted" عند النقر المتكرر.
        if widget.parent() is not None:
            # 1. إرسال إشارة لحذف العنصر من قائمة السجل الرئيسية.
            self.delete_entry_requested.emit(entry)
            # 2. إزالة الويدجت من التنسيق وإخفائه.
            self.log_layout.removeWidget(widget)
            widget.setParent(None)
            widget.deleteLater()

class MainWindow(QMainWindow):
    def __init__(self, initial_arg=None):
        super().__init__()
        self.setWindowTitle("Mostafa Internet Downloader 🚀")
        
        self.setWindowFlags(Qt.FramelessWindowHint) # لجعل النافذة بدون إطار قياسي
        self.server = None # سيتم تهيئته لاحقاً لنظام النسخة الواحدة

        # --- تعيين أيقونة البرنامج ---
        if os.path.exists('icon.ico'):
            self.setWindowIcon(QIcon('icon.ico'))

        # --- إعدادات المظهر ---
        self.setStyleSheet("""
            /* --- Modern Dark Theme by Gemini Code Assist --- */
            QMainWindow, QDialog {
                background-color: #1e1f22; /* لون الخلفية الرئيسي */
            }
            QWidget {
                background-color: #2b2d31; /* لون الخلفية العام */
                color: #f2f3f5; /* لون النص الأساسي */
                font-size: 14px;
                font-family: "Segoe UI", "Roboto", "Helvetica Neue", Arial, sans-serif;
            }
            QLineEdit {
                padding: 8px;
                border: 1px solid #1e1f22;
                border-radius: 5px;
                background-color: #1e1f22;
                selection-background-color: #5865f2;
            }
            QPushButton {
                background-color: #5865f2; /* لون الزر الرئيسي (بنفسجي) */
                color: white;
                padding: 8px;
                border: none;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #4e5bf0;
            }
            QPushButton:disabled {
                background-color: #4f545c;
                color: #96989d;
            }
            /* --- أزرار التحكم في التحميل --- */
            #ControlButton {
                background-color: #4f545c;
                font-size: 18px; /* تكبير حجم الأيقونات */
                padding: 6px 12px;
            }
            #ControlButton:hover { background-color: #5d636b; }
            #CancelButton { background-color: #da373c; }
            #CancelButton:hover { background-color: #f04747; }
            
            QProgressBar {
                border: none;
                border-radius: 5px;
                text-align: center;
                background-color: #1e1f22;
                color: #f2f3f5;
            }
            QProgressBar::chunk {
                background-color: #248046; /* أخضر للتقدم */
                border-radius: 5px;
            }
            QFrame#DownloadTaskWidget { /* تطبيق النمط على عنصر التحميل فقط */
                background-color: #383a40;
                border-radius: 8px;
                padding: 8px;
            }
            QTabWidget::pane { /* The content area of the tab widget */
                border: 1px solid #555;
                border-top: none;
            }
            QTabBar::tab {
                background-color: #2b2d31;
                color: #b8bac0;
                padding: 10px;
                min-width: 150px;
                border: 1px solid #555;
                border-bottom: none; /* To merge with the pane */
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background-color: #383a40;
                color: white;
                font-weight: bold;
            }
            QTabBar::tab:!selected:hover {
                background-color: #313338;
            }
        """)

        # --- منطق التشغيل الأساسي وتحميل الإعدادات ---
        # يجب أن يتم هذا قبل إنشاء الواجهات التي قد تعتمد على هذه الإعدادات
        self.thread_pool = QThreadPool()
        self.network_monitor = None
        self.tasks = {} # {task_id: widget}
        self.active_downloads = 0 # عدد التحميلات النشطة حالياً
        self.pending_queue = [] # (worker, widget) قائمة انتظار للتحميلات
        self.MAX_CONCURRENT_DOWNLOADS = 3 # قيمة افتراضية، سيتم استبدالها من الإعدادات
        self.is_fetching = False # Flag to track if we are fetching details
        self.completed_log = [] # List to store completed download messages
        
        # --- تحميل الإعدادات وتعيين مجلد التحميلات ---
        self.settings = self.load_settings()
        self._load_completed_log() # --- إصلاح: نقل تحميل السجل إلى ما بعد تحميل الإعدادات ---
        self.download_folder = self.settings.get("download_folder", "downloads")
        self.MAX_CONCURRENT_DOWNLOADS = self.settings.get("max_concurrent_downloads", 3)
        self.telegram_client = None # For Telethon
        os.makedirs(self.download_folder, exist_ok=True)

        # --- الواجهة الرئيسية (بدون إطار) ---
        self.central_container = QWidget()
        self.setCentralWidget(self.central_container)
        self.main_layout = QVBoxLayout(self.central_container)
        self.main_layout.setContentsMargins(1, 1, 1, 1) # هامش صغير للإطار

        # --- نظام الصفحات (Tabs) ---
        self.tabs = QTabWidget()

        # --- إنشاء صفحة التحميلات ---
        # --- IMPORTANT: create downloader UI before using any of its widgets ---

        self.downloader_page = QWidget()
        self._create_downloader_ui(self.downloader_page)
        self.tabs.addTab(self.downloader_page, "⬇️ التحميلات")

        # --- إنشاء صفحة تحويل الصيغ ---
        self.converter_page = QWidget()
        self._create_converter_ui(self.converter_page)
        self.tabs.addTab(self.converter_page, "🧰 الأدوات")

        # --- إنشاء صفحة الإعدادات ---
        self.settings_page = QWidget()
        self._create_settings_ui(self.settings_page)
        self.tabs.addTab(self.settings_page, "⚙️ الإعدادات")

        # --- إنشاء صفحة مدير تيليجرام ---
        if TELETHON_AVAILABLE:
            self.telegram_page = QWidget()
            self._create_telegram_manager_ui(self.telegram_page)
            self.tabs.addTab(self.telegram_page, "🗂️ مدير تيليجرام")
        

        # --- إضافة شريط العنوان المخصص والصفحات ---
        self._create_title_bar()
        self.main_layout.addWidget(self.tabs)

        # --- إضافة شريط الحالة السفلي ---
        status_bar_layout = QHBoxLayout()
        status_bar_layout.setContentsMargins(10, 2, 10, 2) # هوامش صغيرة

        # إضافة اسم المطور في الجهة اليسرى
        self.developer_label = QLabel("By Mostafa Soft")
        self.developer_label.setStyleSheet("color: #96989d; font-size: 12px;")
        status_bar_layout.addWidget(self.developer_label)

        status_bar_layout.addStretch() # مسافة مرنة في المنتصف
        self.main_layout.addLayout(status_bar_layout)

        self.last_task_id = 0 # عداد لتوليد أرقام تعريف آمنة وفريدة
        self.service_domains = ['youtube.com', 'youtu.be', 'facebook.com', 'twitter.com', 'x.com', 'instagram.com', 'tiktok.com', 't.me', 'threads.com', 'dailymotion.com', 'dai.ly']
        self.FORBIDDEN_KEYWORDS = [
            # English keywords - More specific and less prone to false positives
            "porn", "sex", "xxx", "adult", "hentai", "erotic", "nude", "nudity", "nsfw", "18+", "18plus", "adult content", "adult videos", "adult movies", "adult games", "adult sites", "adult entertainment", "erotica", "stripper", "strip club", "striptease", "camgirl", "live cam", "lesbian", "gay", "homosexual", "homo", "lesbo", "bisexual", "sexual", "Sweetie Fox", "sweetie fox", " Sweetie Fox ", "sweetie fox ", "SweetieFox", "sweetiefox", "Sweetie Fox ", "red room", "dark web", "deep web", "RED ROOM", " RED ROOM ", " RED ROOM", "RED ROOM ", "REDROOM"
            "gambling", "casino", "betting", "poker", "redtube", "xvideos", "youjizz", "xhamster", "pornhub", "blowjob", "masturbation", "naked", "striptease", "cumshot", "anal", "oral", "vagina", "penis", "breast", "tits", "ass", "buttocks", "cum", "fap", "fapping", "pussy", "cock", "dick", "xnxx", "xnxx.com", "4kporn", "horny", "sexy", "+18", "nutaku", "hentaihaven", "hentaifox", "hentaiporn", "hentaitube", "hentaigasm", "hentaigallery", "hentaicomics", "hentaianime", "hentaivideos", "hentaipornvideos", "hentaipornhub", "hentaipornsite",
            "www.pornhub.com", "www.xvideos.com", "www.redtube.com", "www.youjizz.com", "www.xhamster.com", "www.tube8.com", "www.spankwire.com", "www.hqporner.com", "www.youporn.com", "www.porn300.com", "www.pornhd.com", "www.porn3000.com", "www.porntrex.com", "www.porn555.com", "www.porn5000.com", "sweetiefox",  "sweetie-fox", "hentaifox.com", "hentaifox net", "hentaifox org", "hentaifox xyz", "hentaifox io", "hentaifox me", "hentaifox tv", "hentaifox online", "hentaifox site", "hentaifox blog", "hentaifox app", "SweetieFox", "Sweetie-fox", "hentaifox", "hentaifox.com", "hentaifox.net", "hentaifox.org", "hentaifox.xyz", "hentaifox.io", "hentaifox.me", "hentaifox.tv", "hentaifox.online", "hentaifox.site", "hentaifox.blog", "hentaifox.app",
            # Arabic keywords
            "شواذ", "شاذ", "قضيب", "مهبل", "طيز", "بزاز", "ينيك", "تتناك", "شرموطه", "شرموطة", "قحبه", "قحبة", "سكس", "زبر", "كس", "اباحي", "جنس", "قمار", "رهان"
        ]

        # --- ربط الإشارات ---

        # إذا تم تمرير رابط عند بدء التشغيل
        if initial_arg:
            QTimer.singleShot(100, lambda: self.handle_startup_argument(initial_arg))

        self.load_incomplete_downloads()
        self.start_network_monitor()

        # --- ربط تغيير الصفحة لتحديث مدير تيليجرام ---
        self.tabs.currentChanged.connect(self.on_tab_changed)

    def _create_title_bar(self):
        """Creates a custom title bar with window controls and extra buttons."""
        self.title_bar = QWidget()
        self.title_bar.setStyleSheet("background-color: #1e1f22; height: 40px;")
        title_layout = QHBoxLayout(self.title_bar)
        title_layout.setContentsMargins(10, 0, 0, 0)
        title_layout.setSpacing(10)

        # Title and Icon
        icon_label = QLabel()
        if os.path.exists('icon.ico'):
            icon_label.setPixmap(QIcon('icon.ico').pixmap(24, 24))
        title_label = QLabel("Mostafa Internet Downloader 🚀")
        title_label.setStyleSheet("font-weight: bold; color: #f2f3f5;")
        
        title_layout.addWidget(icon_label)
        title_layout.addWidget(title_label)
        title_layout.addStretch()

        # --- أزرار إضافية في شريط العنوان ---
        self.log_button = QPushButton("📜")
        self.log_button.setToolTip("سجل التحميلات")
        self.log_button.setStyleSheet("background-color: transparent; font-size: 18px;")
        self.log_button.clicked.connect(self.open_log_dialog)
        title_layout.addWidget(self.log_button)

        # Window control buttons
        self.minimize_button = QPushButton("—")
        self.maximize_button = QPushButton("🗗")
        self.close_button = QPushButton("✕")
        self.minimize_button.setFixedSize(40, 30)
        self.maximize_button.setFixedSize(40, 30)
        self.close_button.setFixedSize(40, 30)
        self.minimize_button.setStyleSheet("background-color: transparent; border: none;")
        self.maximize_button.setStyleSheet("background-color: transparent; border: none;")
        self.close_button.setStyleSheet("background-color: transparent; border: none; color: #f2f3f5;")
        self.close_button.setObjectName("closeButton") # For hover style
        self.title_bar.setStyleSheet("QPushButton#closeButton:hover { background-color: #da373c; }")

        title_layout.addWidget(self.minimize_button)
        title_layout.addWidget(self.maximize_button)
        title_layout.addWidget(self.close_button)

        self.minimize_button.clicked.connect(self.showMinimized)
        self.maximize_button.clicked.connect(self._toggle_maximize)
        self.close_button.clicked.connect(self.close)

        self.main_layout.addWidget(self.title_bar)

    def _create_downloader_ui(self, parent_widget):
        """إنشاء كل عناصر واجهة صفحة التحميلات"""
        main_layout = QVBoxLayout(parent_widget)

        # --- الجزء العلوي (إدخال الرابط والزر) ---
        top_layout = QHBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("🔗 الصق رابط التحميل هنا...")
        self.cancel_fetch_button = QPushButton("❌")
        self.download_button = QPushButton("➕")
        self.download_button.setToolTip("بدء تحميل الرابط")
        self.download_button.setFixedSize(50, 40)

        top_layout.addWidget(self.url_input)
        top_layout.addWidget(self.download_button)
        self.cancel_fetch_button.setToolTip("إلغاء فحص الرابط")
        self.cancel_fetch_button.setFixedSize(50, 40)
        top_layout.addWidget(self.cancel_fetch_button)

        self.fetch_status_label = QLabel("جاهز")
        top_layout.addWidget(self.fetch_status_label)
        self.cancel_fetch_button.setEnabled(False)

        main_layout.addLayout(top_layout)

        # --- الجزء الأوسط (أزرار التحكم الشاملة) ---
        global_controls_layout = QHBoxLayout()
        self.pause_all_button = QPushButton("⏸️ إيقاف الكل مؤقتاً")
        self.cancel_all_button = QPushButton("❌ إلغاء الكل")
        self.pause_all_button.setStyleSheet("background-color: #e67e22; color: white;") # Orange
        self.cancel_all_button.setStyleSheet("background-color: #c0392b;") # Darker Red

        global_controls_layout.addStretch()
        global_controls_layout.addWidget(self.pause_all_button)
        global_controls_layout.addWidget(self.cancel_all_button)
        global_controls_layout.addStretch()
        main_layout.addLayout(global_controls_layout)


        # --- منطقة عرض التحميلات (قابلة للتمرير) ---
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content_widget = QWidget()
        self.download_list_layout = QVBoxLayout(scroll_content_widget)
        self.download_list_layout.setAlignment(Qt.AlignTop)
        scroll_area.setWidget(scroll_content_widget)
        main_layout.addWidget(scroll_area)

        # --- الجزء السفلي (أزرار إضافية) ---
        bottom_layout = QHBoxLayout()
        self.library_button = QPushButton("📚 مكتبة الفيديوهات")
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.library_button)
        bottom_layout.addStretch()
        main_layout.addLayout(bottom_layout)

        # --- ربط الإشارات بعد إنشاء الواجهة ---
        self.download_button.clicked.connect(self.start_download)
        self.url_input.returnPressed.connect(self.start_download)
        self.cancel_fetch_button.clicked.connect(self.cancel_fetch)
        self.pause_all_button.clicked.connect(self.pause_all_tasks)
        self.cancel_all_button.clicked.connect(self.show_cancel_all_dialog)
        self.library_button.clicked.connect(self.open_video_library)
        self.fetch_with_telethon_running = False

    def cancel_fetch(self):
      print("Cancel_fetch triggred")
      self.fetch_with_telethon_running = False
      self.TelethonDirectFetcherRunning = False
      self._restore_fetch_ui()

    def _sanitize_filename(self, filename):
        return re.sub(r'[\\/*?:"<>|]', "", filename)

    def _toggle_maximize(self):
        if self.isMaximized():
            self.showNormal()
        else:
            self.showMaximized()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.old_pos = event.globalPosition().toPoint()

    def mouseMoveEvent(self, event):
        if hasattr(self, 'old_pos'):
            delta = event.globalPosition().toPoint() - self.old_pos
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self.old_pos = event.globalPosition().toPoint()

    def mouseReleaseEvent(self, event):
        # --- FIX: Check if old_pos exists before deleting ---
        # This prevents a crash if a mouse button other than LeftButton was released.
        if hasattr(self, 'old_pos'):
            del self.old_pos

    def start_download(self):
        if self.is_fetching:
            return # منع الضغط المتكرر

        url = self.url_input.text().strip()
        if not url:
            return

        # --- FIX: Handle Threads URLs by appending /embed ---
        # This makes yt-dlp recognize and process them correctly.
        if "threads.net" in url or "threads.com" in url:
            if not url.endswith('/embed'):
                url = url.split('?')[0] + '/embed'

        # --- التعامل مع روابط Telethon الداخلية بشكل خاص ---
        if url.startswith("telethon://"):
            QMessageBox.warning(self, "خطأ", "لا يمكن تحميل هذا الرابط مباشرة. يرجى لصق رابط القناة الأصلي.")
            return

        # --- فلتر المحتوى الممنوع (فحص الرابط) ---
        if self._is_content_forbidden(url):
            QMessageBox.critical(self, "محتوى ممنوع", "ممنوع تنزيل هذا النوع من الملفات، اتقِ الله.")
            self.url_input.clear()
            return

        is_service_url = any(domain in url for domain in self.service_domains) or "/embed" in url

        self.fetch_with_telethon_running = False
        self.cancel_fetch_button.setEnabled(True)

        self.url_input.setEnabled(False)
        self.download_button.setEnabled(False) #disable download button
        self.fetch_status_label.setText("جاري فحص الرابط...")
        self.is_fetching = True

        if is_service_url and YTDLP_AVAILABLE:
            tg_settings = self.settings.get("telegram", {})
            # --- منطق جديد: استخدام Telethon فقط للقنوات الخاصة (التي لا تحتوي على /s/) ---
            # إذا كان الرابط لقناة خاصة وبيانات Telethon موجودة، استخدم Telethon.
            if 't.me/' in url and '/s/' not in url and all(tg_settings.get(k) for k in ["api_id", "api_hash"]):
                self.fetch_with_telethon(url, tg_settings) # هذه الدالة تبدأ الخيط بنفسها
                return

            # --- جديد: التحقق من روابط قنوات وقوائم تشغيل يوتيوب ---
            is_youtube_playlist = 'youtube.com/' in url and ('/playlist?' in url or '/channel/' in url or '/c/' in url or '/user/' in url)
            if is_youtube_playlist:
                # 1. إنشاء وإظهار نافذة الاختيار الفارغة فوراً
                self.playlist_dialog = PlaylistSelectionDialog(parent=self)
                self.playlist_dialog.videos_selected.connect(self.handle_playlist_selection)
                self.playlist_dialog.show()
                # 2. بدء العامل لجلب الفيديوهات تدريجياً
                fetcher = YTDLPlaylistFetcher(url)
                fetcher.signals.yt_video_found.connect(self.on_yt_video_found)
                fetcher.signals.yt_playlist_fetch_finished.connect(self.on_yt_playlist_fetch_finished)
                fetcher.signals.fetch_error.connect(self.on_yt_playlist_fetch_error)
                self.thread_pool.start(fetcher)
                return

            # --- المنطق الافتراضي: استخدام yt-dlp لجميع الروابط الأخرى (يوتيوب، فيسبوك، وقنوات تيليجرام العامة) ---
            # yt-dlp سريع ويدعم التحميل متعدد الأجزاء.
            playlist_url = url
            if 't.me/' in url and '/s/' not in url:
                parsed_url = urlparse(url)
                path_parts = parsed_url.path.strip('/').split('/')
                if len(path_parts) == 2 and path_parts[1].isdigit():
                    # yt-dlp can handle this directly
                    playlist_url = url
                else: # رابط قناة عامة كاملة
                    channel_name = path_parts[0]
                    playlist_url = f"https://t.me/s/{channel_name}"



            fetcher = YTDLInfoFetcher(url)
            fetcher.signals.info_fetched.connect(self.on_ytdlp_info_fetched)

            fetcher.signals.fetch_error.connect(self.on_fetch_error)
            self.thread_pool.start(fetcher)
        else:
            fetcher = DetailsFetcher(url)
            fetcher.signals.details_fetched.connect(self.create_direct_download_task_ui)
            fetcher.signals.fetch_error.connect(self.on_fetch_error)
            self.thread_pool.start(fetcher)

    def fetch_with_telethon(self, url, tg_settings):
        print("Starting telegram fetch with Telethon ")
        self.fetch_with_telethon_running = True
        # Helper function to decide whether to fetch a single post or a whole channel with Telethon.
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip('/').split('/')
        self.TelethonDirectFetcherRunning=False


        # If the link has two parts (channel_name/post_id), it's a direct post link
        if len(path_parts) == 2 and path_parts[1].isdigit():
            print("Starting  telethon fetch Direect with Telethon ")
            self.TelethonDirectFetcherRunning = True
            # --- تعديل: استخدام العامل الجديد من downloader_core ---
            # لم نعد بحاجة إلى QRunnable هنا، بل نستخدم خيط بايثون عادي
            # ونمرر له دوال callback لإرجاع النتيجة.
            fetcher = TelethonDirectFetcher(
                url=url, api_id=tg_settings["api_id"], api_hash=tg_settings["api_hash"],
                session_string=tg_settings.get("session_string"),
                result_callback=self.create_direct_download_task_ui,
                error_callback=self.on_fetch_error
            )
            fetcher.start() # بدء الخيط
        else: # It's a full channel link
            fetcher = TelethonFetcher(
                url, tg_settings["api_id"], tg_settings["api_hash"],
                tg_settings.get("phone"), tg_settings.get("session_string")
            )
            fetcher.signals.playlist_info_fetched.connect(self.on_playlist_info_fetched)
            fetcher.signals.fetch_error.connect(self.on_fetch_error)
            
        self.thread_pool.start(fetcher)

    def _is_content_forbidden(self, text_to_check):
        text_lower = text_to_check.lower()
        return any(re.search(r'\b' + re.escape(keyword) + r'\b', text_lower) for keyword in self.FORBIDDEN_KEYWORDS)

    @Slot(str, str, object)
    def create_direct_download_task_ui(self, url, filename, total_size):
        # إعادة تفعيل الواجهة
        if not self.is_fetching: return # The user cancelled

        if filename is None:
            QMessageBox.critical(self, "خطأ", "فشل جلب معلومات الملف.")
            self._restore_fetch_ui()
            return

        # --- فلتر المحتوى الممنوع (فحص اسم الملف) ---
        if self._is_content_forbidden(filename):
            QMessageBox.critical(self, "محتوى ممنوع", "ممنوع تنزيل هذا النوع من الملفات، اتقِ الله.")
            self._restore_fetch_ui()
            return

        # --- معالجة خاصة لروابط Telethon الداخلية ---
        if url.startswith("telethon://"):
            self.last_task_id += 1
            task_id = self.last_task_id
            tg_settings = self.settings.get("telegram", {})
            # Sanitize filename to prevent path issues
            safe_filename = self._sanitize_filename(filename)
            worker = TelethonDownloadWorker(task_id, url, self.download_folder, safe_filename, tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
            widget = DownloadTaskWidget(safe_filename, task_id, worker)
            self._setup_and_queue_worker(worker, widget)
            self._restore_fetch_ui()
            return

        # --- للروابط المباشرة العادية ---
        self.last_task_id += 1
        task_id = self.last_task_id
        filepath = find_unique_filepath(self.download_folder, filename)

        # 4. إنشاء العامل (Worker)
        worker = CoreDownloadWorker(task_id, url, filepath, total_size)

        # 5. إنشاء الواجهة الخاصة به (هذا الآن آمن لأنه يحدث في الخيط الرئيسي)
        widget = DownloadTaskWidget(filename, task_id, worker)

        # If total_size is None, it's an indeterminate download.
        # Pause/Resume is not supported, so disable the button.
        if total_size is None:
            widget.toggle_button.setEnabled(False)
            widget.toggle_button.setText("...")
        widget.play_requested.connect(self.launch_player)
        self.download_list_layout.insertWidget(0, widget)
        self.tasks[task_id] = widget
        
        # 6. إضافة المهمة إلى قائمة الانتظار وبدء التشغيل إذا كان هناك مكان
        self._add_to_queue_and_start(worker, widget)

        self._restore_fetch_ui() # Restore UI for the next download

    @Slot(str, dict)
    def on_ytdlp_info_fetched(self, url, info):
        if not self.is_fetching: return # The user cancelled

        title = info.get('title', 'download')
        if self._is_content_forbidden(title) or self._is_content_forbidden(url):
            QMessageBox.critical(self, "محتوى ممنوع", "ممنوع تنزيل هذا النوع من الملفات، اتقِ الله.")
            self._restore_fetch_ui()
            return

        # --- جديد: التعامل مع الصور مباشرة ---
        # إذا كان الرابط لصورة (مثل Instagram)، فإن yt-dlp يضع رابط الصورة مباشرة في 'url'.
        # في هذه الحالة، لا يوجد 'formats' أو 'entries'.
        if info.get('url') and not info.get('formats') and not info.get('entries'):
            # نبدأ تحميل الصورة مباشرة كتحميل عادي.
            self.create_direct_download_task_ui(info['url'], self._sanitize_filename(title) + '.jpg', None)
            return

        formats = info.get('formats')

        # --- جديد: سؤال المستخدم عن نوع التحميل (فيديو أو صوت) ---
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("اختر نوع التحميل")
        msg_box.setText(f"ماذا تريد أن تحمل من:\n'{title}'؟")
        msg_box.setIcon(QMessageBox.Question)

        video_button = msg_box.addButton("🎬 فيديو (بأفضل جودة)", QMessageBox.ActionRole)
        audio_button = msg_box.addButton("🎵 صوت فقط (MP3)", QMessageBox.ActionRole)
        cancel_button = msg_box.addButton("إلغاء", QMessageBox.RejectRole)
        msg_box.exec()

        clicked_button = msg_box.clickedButton()

        if clicked_button == video_button:
            # --- تعديل: تحميل أفضل جودة مباشرة لبعض المواقع لتجنب الأخطاء ---
            # Dailymotion: لتجنب خطأ "Requested format is not available".
            # TikTok, Threads, X.com: للحصول على أفضل توافقية.
            direct_download_sites = ['dailymotion.com', 'tiktok.com', 'threads.net', 'threads.com', 'x.com']
            if any(domain in url for domain in direct_download_sites):
                self.create_ytdl_task_ui(url, info, None, audio_only=False)
                return

            # --- للمواقع الأخرى (مثل يوتيوب)، اعرض قائمة الجودات ---
            if formats:
                filtered_formats = QualitySelectionDialog._filter_and_sort_formats(formats)
                dialog = QualitySelectionDialog(filtered_formats, self)
                dialog.format_selected.connect(lambda format_id: self.create_ytdl_task_ui(url, info, format_id, audio_only=False))
                if not dialog.exec(): self._restore_fetch_ui()
            else:
                self.create_ytdl_task_ui(url, info, None, audio_only=False)
        elif clicked_button == audio_button:
            self.create_ytdl_task_ui(url, info, None, audio_only=True)
        else: # User cancelled
            self._restore_fetch_ui()

    @Slot(str, dict)
    def on_playlist_info_fetched(self, url, info):
        if not self.is_fetching: return

        entries = info.get('entries')
        if not entries:
            self.on_fetch_error("لم يتم العثور على فيديوهات في القناة. قد تكون القناة خاصة أو الرابط غير صحيح.")
            return
        
        self.playlist_dialog = PlaylistSelectionDialog(info, self)
        # --- تعديل: الربط مع الإشارة الجديدة التي ترسل قائمة ---
        self.playlist_dialog.videos_selected.connect(self.handle_playlist_selection)
        if not self.playlist_dialog.exec():
            self._restore_fetch_ui() # User cancelled the dialog

    @Slot(dict)
    def on_yt_video_found(self, entry):
        """يتم استدعاؤها لكل فيديو يوتيوب يتم العثور عليه، وتضيفه إلى نافذة الاختيار."""
        if self.playlist_dialog and self.playlist_dialog.isVisible():
            self.playlist_dialog.add_video_entry(entry)

    @Slot(str, int)
    def on_yt_playlist_fetch_finished(self, title, count):
        """يتم استدعاؤها عند اكتمال جلب قائمة تشغيل يوتيوب."""
        if self.playlist_dialog and self.playlist_dialog.isVisible():
            self.playlist_dialog.set_title_and_finish(f"اكتمل البحث في: {title}")
        # لا نعيد الواجهة هنا، لأن نافذة الاختيار لا تزال مفتوحة
        # يتم استعادة الواجهة عند إغلاق نافذة الاختيار
        self.is_fetching = False
        self.cancel_fetch_button.setEnabled(False)
        self.fetch_status_label.setText("اكتمل البحث. اختر من النافذة.")

    @Slot(str)
    def on_yt_playlist_fetch_error(self, message):
        """Handles errors specifically from the YouTube playlist fetcher."""
        if self.playlist_dialog and self.playlist_dialog.isVisible():
            self.playlist_dialog.close() # أغلق النافذة الفارغة عند حدوث خطأ
        self.on_fetch_error(message) # ثم اعرض رسالة الخطأ

    def handle_playlist_selection(self, video_urls):
        """
        Handles the list of URLs selected from a playlist dialog.
        For YouTube, it asks for quality once. For others, it processes them one by one.
        """
        self._restore_fetch_ui()

        if not video_urls:
            return

        # --- جديد: منطق مخصص لقوائم يوتيوب ---
        is_youtube = 'youtube.com' in video_urls[0] or 'youtu.be' in video_urls[0]
        if is_youtube:
            self.pending_playlist_urls = video_urls
            self.fetch_status_label.setText("جاري جلب خيارات الجودة...")
            # جلب الجودات لأول فيديو فقط كسند للكل
            fetcher = YTDLInfoFetcher(video_urls[0])
            fetcher.signals.info_fetched.connect(self.on_playlist_quality_info_fetched)
            fetcher.signals.fetch_error.connect(self.on_fetch_error)
            self.thread_pool.start(fetcher)
        else:
            # --- المنطق القديم للمنصات الأخرى (مثل تيليجرام) ---
            for video_url in video_urls:
                if video_url.startswith("telethon://"):
                    self._create_telethon_task_from_url(video_url)

    def _create_telethon_task_from_url(self, video_url):
        """Helper function to create a single Telethon download task from a URL."""
        self.last_task_id += 1
        task_id = self.last_task_id

        tg_settings = self.settings.get("telegram", {})
        
        # Find the selected entry to get the filename
        dialog = self.findChild(PlaylistSelectionDialog)
        selected_entry = next((e for e in dialog.entries if e['url'] == video_url), None) if dialog else None
        
        # Sanitize filename to prevent path issues
        raw_filename = selected_entry.get('filename', f"telegram_file_{task_id}") if selected_entry else f"telegram_file_{task_id}"
        safe_filename = self._sanitize_filename(raw_filename)
        unique_filepath = find_unique_filepath(self.download_folder, safe_filename)

        worker = TelethonDownloadWorker(task_id, video_url, self.download_folder, os.path.basename(unique_filepath), tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
        widget = DownloadTaskWidget(os.path.basename(unique_filepath), task_id, worker)
        self._setup_and_queue_worker(worker, widget)

    @Slot(str, dict)
    def on_playlist_quality_info_fetched(self, url, info):
        """
        يتم استدعاؤها بعد جلب جودات أول فيديو في القائمة.
        تفتح نافذة اختيار الجودة مرة واحدة للكل.
        """
        formats = info.get('formats')
        if not formats:
            self.on_fetch_error("لم يتم العثور على جودات متاحة. قد يكون الفيديو غير متاح.")
            return

        filtered_formats = QualitySelectionDialog._filter_and_sort_formats(formats)
        dialog = QualitySelectionDialog(filtered_formats, self)
        dialog.setWindowTitle("اختر جودة لتحميل كل الفيديوهات المحددة")
        # ربط اختيار الجودة ببدء التحميل الدفعي
        dialog.format_selected.connect(self.start_batch_ytdl_downloads)
        if not dialog.exec():
            self._restore_fetch_ui() # ألغى المستخدم العملية

    def start_batch_ytdl_downloads(self, format_id):
        """يبدأ تحميل كل الفيديوهات في القائمة المعلقة بنفس الجودة المحددة."""
        for video_url in self.pending_playlist_urls:
            # لا نحتاج لجلب المعلومات مرة أخرى، ننشئ المهمة مباشرة
            self.create_ytdl_task_ui(video_url, {}, format_id, audio_only=False)
        self.pending_playlist_urls = [] # تفريغ القائمة بعد البدء

    def create_ytdl_task_ui(self, url, info, format_id, audio_only=False):
        self.last_task_id += 1
        task_id = self.last_task_id
        
        title = info.get('title', f'download_{task_id}')

        # إنشاء العامل
        worker = YTDLDownloadWorker(task_id, url, self.download_folder, format_id, audio_only)

        # إنشاء الواجهة
        widget = DownloadTaskWidget(title, task_id, worker)
        self._setup_and_queue_worker(worker, widget)
        self._restore_fetch_ui() # Restore UI for the next download
    
    def _create_resumable_task_from_state(self, filepath, data):
        """Helper function to create a UI task for a resumable download from state data."""
        if not isinstance(data, dict) or not all(k in data for k in ['url', 'total_size', 'segments']):
            return

        url = data['url']
        total_size = data['total_size']
        segments = data['segments']
        downloaded_size = sum(segments)
        percent = int((downloaded_size / total_size) * 100) if total_size > 0 else 0
        filename = os.path.basename(filepath)

        self.last_task_id += 1
        task_id = self.last_task_id

        # --- FIX: Create worker in repair_mode to force re-check of segments ---
        worker = CoreDownloadWorker(task_id, url, filepath, total_size, start_paused=True, repair_mode=True)
        widget = DownloadTaskWidget(filename, task_id, worker)

        # Set the UI to the paused state
        status_text = f"{filename} - متوقف مؤقتاً ({format_size(downloaded_size)} / {format_size(total_size)})"
        widget.set_paused(status_text)
        widget.update_progress(percent, status_text)

        widget.play_requested.connect(self.launch_player)
        # Add to UI and task map
        self.download_list_layout.insertWidget(0, widget)
        self.tasks[task_id] = widget
        self._add_to_queue_and_start(worker, widget, is_resumed=True)

    def load_incomplete_downloads(self):
        """Loads paused downloads from the state file on startup."""
        state = load_state()
        for filepath, data in state.items():
            self._create_resumable_task_from_state(filepath, data)

    def _setup_and_queue_worker(self, worker, widget):
        """A helper to connect signals, add to UI, and queue a worker."""

        widget.play_requested.connect(self.launch_player)
        self.download_list_layout.insertWidget(0, widget) # Add to top of the list
        self.tasks[worker.task_id] = widget
        self._add_to_queue_and_start(worker, widget)

    def _add_to_queue_and_start(self, worker, widget, is_resumed=False):
        """Adds a task to the queue and starts it if there's a free slot."""
        # ربط الإشارات دائماً
        worker.signals.progress.connect(self.on_progress)
        worker.signals.finished.connect(self.on_finished)
        worker.signals.error.connect(self.on_error)


        worker.signals.cancelled.connect(self.on_cancelled)
        worker.signals.paused.connect(self.on_paused)
        worker.signals.resumed.connect(self.on_resumed)

        if self.active_downloads < self.MAX_CONCURRENT_DOWNLOADS:
            self.active_downloads += 1
            if not is_resumed: # لا تظهر رسالة "جاري البدء" للمهام المستأنفة
                widget.status_label.setText("جاري البدء...")
            self.thread_pool.start(worker)
        else:
            widget.status_label.setText("⏳ في الانتظار...")
            widget.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #7f8c8d; }") # Gray color
            self.pending_queue.append((worker, widget))

    def _start_next_in_queue(self):
        """Checks the queue and starts the next pending download if possible."""
        if self.pending_queue and self.active_downloads < self.MAX_CONCURRENT_DOWNLOADS:

            worker, widget = self.pending_queue.pop(0)
            self.active_downloads += 1
            widget.status_label.setText("جاري البدء...")
            self.thread_pool.start(worker)

    def open_log_dialog(self):
        """Opens the download log dialog."""
        dialog = DownloadLogDialog(self.completed_log, self)
        dialog.retry_requested.connect(self.retry_download_from_log)
        dialog.log_cleared.connect(self.clear_log)
        dialog.exec()

    @Slot()
    def clear_log(self):
        self.completed_log.clear()

    def retry_download_from_log(self, log_entry):
        """Retries a download based on a log entry."""
        url = log_entry.get("url")
        if url:
            self.url_input.setText(url)
            self.start_download()
            # Optionally, close the log dialog
            for widget in QApplication.topLevelWidgets():
                if isinstance(widget, DownloadLogDialog):
                    widget.accept()

    def _load_completed_log(self):
        """Loads the completed downloads log from a JSON file."""
        try:
            # We can store the log inside the main settings file for simplicity
            self.completed_log = self.settings.get("completed_log", [])
        except Exception as e:
            print(f"Could not load completed log: {e}")
            self.completed_log = []

    def _save_completed_log(self):
        """Saves the completed downloads log to the settings file."""
        try:
            self.settings["completed_log"] = self.completed_log[:50] # Save last 50

        except Exception as e:
            print(f"Could not save completed log: {e}")
    def open_video_library(self):
        dialog = VideoLibraryDialog(self.download_folder, self) #Open Video Library triggred
        dialog.play_video_requested.connect(self.launch_player)
        dialog.exec()

    def listen_for_instances(self):
        """
        Starts a local server to detect and communicate with new instances of the application.
        Returns True if this is the first instance, False otherwise.
        """
        server_name = "MostafaDownloaderInstance"
        self.server = QLocalServer(self)

        # Clean up any leftover sockets from a previous crash
        QLocalServer.removeServer(server_name)

        if not self.server.listen(server_name):
            # This indicates a server is already running, which shouldn't happen
            # if removeServer worked, but it's a good fallback.
            return False

        self.server.newConnection.connect(self.handle_new_instance_connection)
        return True

    @Slot()
    def handle_new_instance_connection(self):
        """Handles an argument (URL or file path) sent from a new instance of the app."""
        socket = self.server.nextPendingConnection()
        if socket:
            if socket.waitForReadyRead(500):
                # Read the argument sent from the other instance
                data = socket.readAll().data().decode('utf-8')
                self.handle_startup_argument(data)
            socket.disconnectFromServer()

    def launch_player(self, filepath: str, is_stream: bool = False):
        """
        يفتح الملف المحدد.
        - إذا كان ملفاً محلياً، يستخدم مشغل النظام الافتراضي.
        - إذا كان بثاً (is_stream=True)، يستخدم مشغل الفيديو المدمج.
        """
        if not filepath:
            QMessageBox.critical(self, "خطأ", "مسار الملف أو الرابط فارغ.")

            return
        # إذا كان بثاً، استخدم المشغل المدمج
        if is_stream:
            from player import MediaPlayerWindow
            self.player_window = MediaPlayerWindow()
            self.player_window.load_video(filepath)
            self.player_window.show()
            return

        # إذا كان ملفاً محلياً، استخدم مشغل النظام
        if not os.path.exists(filepath):
            QMessageBox.critical(self, "خطأ", "ملف الفيديو غير موجود أو تم حذفه.")
            return
        
        try:
            if sys.platform == "win32":
                os.startfile(os.path.normpath(filepath))
            elif sys.platform == "darwin": # macOS
                subprocess.Popen(["open", filepath])
            else: # Linux and other Unix-like
                subprocess.Popen(["xdg-open", filepath])
        except Exception as e:
            QMessageBox.critical(self, "خطأ في التشغيل", f"فشل فتح الملف باستخدام المشغل الافتراضي للنظام.\n\nالخطأ: {e}")

    def start_network_monitor(self):
        """Initializes and starts the background network connectivity checker."""
        self.network_monitor = NetworkMonitor()
        # We connect to generic signals and check for our special task_id (-1)
        self.network_monitor.signals.error.connect(self.handle_network_events)
        self.network_monitor.signals.finished.connect(self.handle_network_events)
        self.thread_pool.start(self.network_monitor)

    @Slot(int, str)
    def handle_network_events(self, task_id: int, message: str):
        """
        A slot that receives signals from the NetworkMonitor and routes them.
        """
        if task_id == -1: # This is a network event, not a download task event
            if message == "CONNECTION_LOST":
                self.on_connection_lost()
            elif message == "CONNECTION_RESTORED":
                self.on_connection_restored()

    def handle_startup_argument(self, argument):
        """
        A centralized handler for arguments from any source (CLI, new instance).
        It determines if the argument is a file path to play or a URL to download.
        """
        # Clean up protocol if present
        if argument.startswith('mostafa-dl://'):
            argument = argument[len('mostafa-dl://'):]

        # Check if it's a local file that exists and has a video extension
        VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv']
        is_video_file = os.path.isfile(argument) and any(argument.lower().endswith(ext) for ext in VIDEO_EXTENSIONS)

        if is_video_file:
            self.launch_player(argument)
        else: # Assume it's a URL to download
            self.tabs.setCurrentWidget(self.downloader_page)
            self.url_input.setText(argument)
            self.start_download()
        
        self.showNormal()  # De-minimize if minimized
        self.activateWindow()  # Bring window to front

    @Slot(int, int, str)
    def on_progress(self, task_id, percent, status_text):
        widget = self.tasks.get(task_id)
        if widget:
            widget.update_progress(percent, status_text)

    @Slot(int, str)
    def on_finished(self, task_id, message):
        widget = self.tasks.get(task_id)
        if widget:
            # --- إصلاح: الحصول على الحجم الصحيح والدقيق للملف ---
            # بدلاً من الاعتماد على حجم الملف على القرص، نأخذ الحجم المتوقع من العامل نفسه.
            # هذا يضمن دقة الحجم حتى لو كان الملف قد تم حذفه أو لم يكتمل بشكل صحيح.
            final_size = 0
            if hasattr(widget.worker, 'task_logic') and hasattr(widget.worker.task_logic, 'total_size'):
                final_size = widget.worker.task_logic.total_size
            elif hasattr(widget.worker, 'total_size'): # For Telethon worker
                final_size = widget.worker.total_size

            final_filepath = widget._get_final_filepath()
            
            log_entry = {
                "timestamp": time.time(),
                "status": "finished",
                "filename": widget.filename_label.text(),
                "status_text": "✅ مكتمل",
                "filepath": final_filepath,
                "final_size": final_size,
                "url": widget.worker.url if hasattr(widget.worker, 'url') else (widget.worker.telethon_url if hasattr(widget.worker, 'telethon_url') else None)
            }
            self.completed_log.append(log_entry)

            widget.set_finished(message)
            # --- تعديل: إزالة العنصر بعد فترة ---
            QTimer.singleShot(5000, lambda: self.remove_task_widget(task_id))

        self.active_downloads -= 1
        self._start_next_in_queue()

    def remove_task_widget(self, task_id):
        self.on_cancelled(task_id)

    @Slot(int, str)
    def on_error(self, task_id, message):
        # --- FIX: Check if task still exists before updating UI ---
        # This prevents a crash if the task was cancelled and its widget deleted
        # just before the error signal was processed.
        widget = self.tasks.get(task_id)
        if widget:
            # --- إصلاح: الحصول على الحجم الصحيح والدقيق للملف حتى في حالة الخطأ ---
            final_size = 0
            if hasattr(widget.worker, 'task_logic') and hasattr(widget.worker.task_logic, 'total_size'):
                final_size = widget.worker.task_logic.total_size
            elif hasattr(widget.worker, 'total_size'): # For Telethon worker
                final_size = widget.worker.total_size

            final_filepath = widget._get_final_filepath()

            log_entry = {
                "timestamp": time.time(),
                "status": "error",
                "filename": widget.filename_label.text(),
                # --- تحسين: إضافة الحجم إلى رسالة الخطأ في السجل ---
                "status_text": message,
                "filepath": final_filepath,
                "final_size": final_size,
                "url": widget.worker.url if hasattr(widget.worker, 'url') else (widget.worker.telethon_url if hasattr(widget.worker, 'telethon_url') else None)
            }
            self.completed_log.append(log_entry)
            widget.set_error(message)
            # --- تعديل: إزالة العنصر بعد فترة ---
            QTimer.singleShot(5000, lambda: self.remove_task_widget(task_id))
        self.active_downloads -= 1
        self._start_next_in_queue()

    def pause_all_tasks(self):
        """Pauses all active and pending downloads."""
        # 1. Pause active downloads
        for widget in self.tasks.values(): # widget is a DownloadTaskWidget
            if widget.is_complete:
                continue

            # --- إصلاح: التحقق من نوع العامل قبل محاولة الإيقاف ---
            # كل عامل لديه طريقته الخاصة في الإيقاف المؤقت.
            is_paused = getattr(getattr(widget.worker, 'task_logic', None), 'is_paused', None)
            
            # We check if the worker has a toggle_pause_resume method and is not already paused.
            if hasattr(widget.worker, 'toggle_pause_resume') and not is_paused:
                widget.worker.toggle_pause_resume()

        # 2. "Pause" pending downloads (by moving them to the front of a new paused queue)
        # This is a simple way to handle it. A more complex system could have a "paused" state.
        # For now, we just stop them from starting automatically.
        # The on_paused slot already handles freeing up slots, so this should work.
        QMessageBox.information(self, "تم الإيقاف", "تم إيقاف جميع التحميلات النشطة مؤقتاً.")

    def show_cancel_all_dialog(self):
        """Shows a dialog with options to cancel all or cancel with exceptions."""
        if not self.tasks and not self.pending_queue:
            QMessageBox.information(self, "لا توجد تحميلات", "لا توجد تحميلات نشطة أو في الانتظار لإلغائها.")
            return

        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("تأكيد الإلغاء")
        msg_box.setText("اختر طريقة الإلغاء:")
        msg_box.setIcon(QMessageBox.Warning)

        cancel_all_btn = msg_box.addButton("إلغاء الكل", QMessageBox.DestructiveRole)
        cancel_except_btn = msg_box.addButton("إلغاء الكل باستثناء...", QMessageBox.ActionRole)
        msg_box.addButton("رجوع", QMessageBox.RejectRole)
        msg_box.exec()

        clicked_button = msg_box.clickedButton()
        if clicked_button == cancel_all_btn:
            # Cancel every task, both active and pending
            all_task_ids = list(self.tasks.keys())
            for task_id in all_task_ids:
                self.on_cancelled(task_id)

        elif clicked_button == cancel_except_btn:
            # Get all tasks (active and pending) to show in the dialog
            all_tasks_map = {**self.tasks, **{w.task_id: wg for w, wg in self.pending_queue}}
            dialog = CancelSelectionDialog(all_tasks_map, self)
            if dialog.exec():
                tasks_to_cancel = dialog.get_tasks_to_cancel()
                for task_id in tasks_to_cancel:
                    self.on_cancelled(task_id)

    @Slot(int, str)
    def on_paused(self, task_id, message):
        widget = self.tasks.get(task_id)
        if widget:
            widget.set_paused(message)
        
        # --- التحسين المطلوب: بدء التحميل التالي عند الإيقاف المؤقت ---
        # إذا كان هناك تحميلات في الانتظار، فإن إيقاف تحميل حالي يحرر مكاناً لآخر.
        if self.active_downloads > 0:
            self.active_downloads -= 1
        self._start_next_in_queue()

    @Slot(int)
    def on_resumed(self, task_id):
        widget = self.tasks.get(task_id)
        if widget:
            widget.set_resumed()

        # --- التحسين المطلوب: زيادة عدد التحميلات النشطة عند الاستئناف ---
        if self.active_downloads < self.MAX_CONCURRENT_DOWNLOADS:
            self.active_downloads += 1

    @Slot(int)
    def on_cancelled(self, task_id):
        widget = self.tasks.get(task_id)
        if widget:
            # --- الإصلاح: التعامل مع منطق الإلغاء بشكل موحد ---
            # أولاً، تحقق مما إذا كان التحميل في قائمة الانتظار وقم بإزالته.
            # هذا يمنع بدء التحميل بعد إلغائه.
            queue_index_to_remove = -1
            for i, (worker, _) in enumerate(self.pending_queue):
                if worker.task_id == task_id:
                    queue_index_to_remove = i
                    break
            if queue_index_to_remove != -1:
                self.pending_queue.pop(queue_index_to_remove)
            else:
                # إذا لم يكن في قائمة الانتظار، فهو إما نشط أو مكتمل/خطأ.
                # إذا كان نشطاً، قلل العداد.
                if not widget.is_complete:
                    self.active_downloads -= 1

            if task_id in self.tasks:
                del self.tasks[task_id]
            widget.deleteLater()
        self._start_next_in_queue()

    def on_connection_lost(self):
        """
        Called when the network connection is lost. Pauses all active, resumable downloads.
        """
        print("Internet connection lost. Pausing active downloads.")
        for widget in self.tasks.values():
            is_resumable = isinstance(widget.worker, CoreDownloadWorker)
            if is_resumable and not widget.is_complete and not widget.worker.task_logic.is_paused:
                widget.auto_paused_by_network = True
                widget.worker.toggle_pause_resume()

    def on_connection_restored(self):
        """
        Called when the network connection is restored. Resumes downloads that were automatically paused.
        """
        print("Internet connection restored. Resuming downloads.")
        for widget in self.tasks.values():
            if widget.auto_paused_by_network:
                widget.auto_paused_by_network = False
                widget.worker.toggle_pause_resume()

    @Slot(str)
    def on_fetch_error(self, message):
        """Handles errors during the info fetching stage and restores the UI."""
        if not self.is_fetching:
            return # Do nothing if the user already cancelled.

        if message:
            QMessageBox.critical(self, "خطأ في جلب المعلومات", message)
        
        self._restore_fetch_ui()

    def _restore_fetch_ui(self, status_message=""):
        # --- إصلاح جذري لمشكلة "Event loop is closed" ---
        # 1. نلغي العامل النشط فقط إذا كان لا يزال قيد التشغيل.
        #    هذا يمنع محاولة إلغاء مهمة انتهت بالفعل، وهو ما يسبب أخطاء.
        if self.active_files_fetcher and self.active_files_fetcher.is_running():
            self.active_files_fetcher.cancel()
            self.active_files_fetcher = None

        # 2. إعادة تعيين جميع الأعلام والمتغيرات المتعلقة بعملية الجلب.
        self.is_fetching = False
        self.fetch_with_telethon_running = False
        self.TelethonDirectFetcherRunning = False

        # 3. إعادة الواجهة إلى حالتها الطبيعية.
        self.url_input.setEnabled(True)
        self.download_button.setEnabled(True)
        self.cancel_fetch_button.setEnabled(False)
        self.fetch_status_label.setText(status_message or "جاهز")
        
    def closeEvent(self, event):
        """Overrides the default close event to handle graceful shutdown."""
        # --- إصلاح جذري لمشكلة "Event loop is closed" عند الإغلاق ---
        # 1. نجمع كل المهام النشطة التي يمكن إيقافها مؤقتاً أو إلغاؤها.
        tasks_to_handle = []
        for task_id, widget in self.tasks.items():
            if not widget.is_complete:
                tasks_to_handle.append(widget.worker)

        if tasks_to_handle:
            print(f"Pausing or cancelling {len(tasks_to_handle)} active task(s) before exiting...")
            # 2. نطلب من كل مهمة أن تتوقف.
            #    - المهام العادية (CoreDownloadWorker) ستحفظ حالتها وتتوقف.
            #    - مهام Telethon/YTDL سيتم إلغاؤها (لأن استئنافها معقد).
            for worker in tasks_to_handle:
                if hasattr(worker, 'pause_for_exit'):
                    worker.pause_for_exit()
                elif hasattr(worker, 'cancel'): # For Telethon/YTDL workers, just cancel them
                    worker.cancel()
            
            # 3. نعطي الخيوط لحظة قصيرة جداً (0.5 ثانية) لتنفيذ أمر الإيقاف وحفظ الحالة.
            #    هذا يمنع إغلاق البرنامج الرئيسي وحلقة الأحداث قبل أن تنتهي الخيوط من عملها.
            print("Waiting for tasks to save state...")
            time.sleep(0.5)

        # Stop the network monitor
        if self.network_monitor:
            self.network_monitor.stop()

        self._save_completed_log()
        self.save_settings()
        print("Exiting.")
        event.accept()


    def _create_converter_ui(self, parent_widget):
        """إنشاء كل عناصر واجهة صفحة تحويل الصيغ"""
        main_layout = QVBoxLayout(parent_widget)
        main_layout.setAlignment(Qt.AlignTop)

        # --- قسم تحويل الصور ---
        img_title = QLabel("تحويل صيغ الصور")
        img_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        img_title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(img_title)

        if not PIL_AVAILABLE:
            msg = QLabel("ميزة تحويل الصور تتطلب: pip install Pillow")
            msg.setStyleSheet("color: orange;")
            main_layout.addWidget(msg)
        else:
            self._create_image_converter_section(main_layout)

        # --- فاصل ---
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        main_layout.addWidget(separator)

        # --- قسم تحويل Excel إلى PDF ---
        excel_title = QLabel("تحويل Excel إلى PDF")
        excel_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        excel_title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(excel_title)

        if not PANDAS_AVAILABLE or not REPORTLAB_AVAILABLE:
            msg = QLabel("ميزة تحويل Excel تتطلب: pip install pandas openpyxl reportlab")
            msg.setStyleSheet("color: orange;")
            main_layout.addWidget(msg)
        else:
            self._create_excel_to_pdf_section(main_layout)

        # --- فاصل ---
        main_layout.addWidget(QFrame(frameShape=QFrame.HLine, frameShadow=QFrame.Sunken))

        # --- قسم دمج الفيديو ---
        vid_title = QLabel("دمج فيديو مع صوت")
        vid_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        vid_title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(vid_title)

        # This container will hold either the install button or the merger controls
        self.video_merger_container = QWidget()
        self.video_merger_layout = QVBoxLayout(self.video_merger_container)
        self.video_merger_layout.setContentsMargins(0,0,0,0)
        main_layout.addWidget(self.video_merger_container)
        
        self._update_merger_ui() # Initial check
        
        # --- فاصل إضافي ---
        separator2 = QFrame()
        separator2.setFrameShape(QFrame.HLine)
        separator2.setFrameShadow(QFrame.Sunken)
        main_layout.addWidget(separator2)

        # --- قسم إصلاح الملفات ---
        self.file_repair_container = QWidget()
        self.file_repair_layout = QVBoxLayout(self.file_repair_container)
        self.file_repair_layout.setContentsMargins(0,0,0,0)
        main_layout.addWidget(self.file_repair_container)
        self._update_repair_ui() # Initial check

        main_layout.addStretch()

    def _update_merger_ui(self):
        """Clears and repopulates the video merger section based on FFMPEG availability."""
        # Clear previous widgets
        while self.video_merger_layout.count():
            child = self.video_merger_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        global FFMPEG_AVAILABLE
        FFMPEG_AVAILABLE = shutil.which('ffmpeg') is not None

        if FFMPEG_AVAILABLE:
            self._create_video_merger_section(self.video_merger_layout)
        else:
            self.ffmpeg_install_button = QPushButton("تحذير: برنامج ffmpeg غير مثبت. اضغط هنا لتثبيته تلقائياً.")
            self.ffmpeg_install_button.setStyleSheet("background-color: #f39c12; color: black;")
            self.ffmpeg_install_button.clicked.connect(self._start_ffmpeg_installation)
            self.video_merger_layout.addWidget(self.ffmpeg_install_button, 0, Qt.AlignCenter)

    def _start_ffmpeg_installation(self):
        self.ffmpeg_install_button.setEnabled(False)
        self.ffmpeg_install_button.setText("جاري التحضير...")
        
        worker = FfmpegInstallerWorker()
        worker.signals.ffmpeg_progress.connect(self.on_ffmpeg_progress)
        worker.signals.ffmpeg_finished.connect(self.on_ffmpeg_finished)
        self.thread_pool.start(worker)

    def _create_image_converter_section(self, layout):
        self.img_conv_path = ""
        
        h_layout1 = QHBoxLayout()
        self.img_conv_select_btn = QPushButton("📂 اختر صورة")
        self.img_conv_path_label = QLabel("لم يتم اختيار ملف")
        h_layout1.addWidget(self.img_conv_select_btn)
        h_layout1.addWidget(self.img_conv_path_label, 1)
        layout.addLayout(h_layout1)

        h_layout2 = QHBoxLayout()
        h_layout2.addWidget(QLabel("اختر الصيغة الجديدة:"))
        self.img_conv_format_combo = QComboBox()
        self.img_conv_format_combo.addItems(["PNG", "JPG", "ICO", "WEBP", "BMP"])
        h_layout2.addWidget(self.img_conv_format_combo)
        h_layout2.addStretch()
        layout.addLayout(h_layout2)

        self.img_conv_start_btn = QPushButton("🔄 ابدأ التحويل")
        self.img_conv_status_label = QLabel("")
        self.img_conv_status_label.setWordWrap(True)
        layout.addWidget(self.img_conv_start_btn, 0, Qt.AlignCenter)
        layout.addWidget(self.img_conv_status_label)

        self.img_conv_select_btn.clicked.connect(self._select_image_for_conversion)
        self.img_conv_start_btn.clicked.connect(self._start_image_conversion)

    def _select_image_for_conversion(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر صورة للتحويل", "", "Image files (*.png *.jpg *.jpeg *.bmp *.webp *.gif)")
        if filepath:
            self.img_conv_path = filepath
            self.img_conv_path_label.setText(os.path.basename(filepath))
            self.img_conv_status_label.setText("")

    def _start_image_conversion(self):
        if not self.img_conv_path:
            QMessageBox.warning(self, "خطأ", "يرجى اختيار صورة أولاً.")
            return
        
        output_folder = os.path.join(self.download_folder, "converted_images")
        os.makedirs(output_folder, exist_ok=True)
        output_format = self.img_conv_format_combo.currentText()
        base_name = os.path.splitext(os.path.basename(self.img_conv_path))[0]
        
        # تعديل بسيط لضمان أن صيغة ICO تكون بأحرف كبيرة كما هو متوقع
        file_extension = output_format if output_format == 'ICO' else output_format.lower()
        output_path = os.path.join(output_folder, f"{base_name}.{file_extension}")

        self.img_conv_status_label.setText("جاري التحويل...")
        worker = ImageConverterWorker(self.img_conv_path, output_path, output_format)
        worker.signals.conversion_finished.connect(lambda msg: self.img_conv_status_label.setText(msg))
        self.thread_pool.start(worker)

    def _create_excel_to_pdf_section(self, layout):
        self.excel_conv_path = ""

        h_layout1 = QHBoxLayout()
        self.excel_conv_select_btn = QPushButton("📂 اختر ملف Excel")
        self.excel_conv_path_label = QLabel("لم يتم اختيار ملف")
        h_layout1.addWidget(self.excel_conv_select_btn)
        h_layout1.addWidget(self.excel_conv_path_label, 1)
        layout.addLayout(h_layout1)

        self.excel_conv_start_btn = QPushButton("🔄 ابدأ التحويل إلى PDF")
        self.excel_conv_status_label = QLabel("")
        self.excel_conv_status_label.setWordWrap(True)
        layout.addWidget(self.excel_conv_start_btn, 0, Qt.AlignCenter)
        layout.addWidget(self.excel_conv_status_label)

        self.excel_conv_select_btn.clicked.connect(self._select_excel_for_conversion)
        self.excel_conv_start_btn.clicked.connect(self._start_excel_to_pdf_conversion)

    def _select_excel_for_conversion(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر ملف Excel", "", "Excel files (*.xlsx *.xls)")
        if filepath:
            self.excel_conv_path = filepath
            self.excel_conv_path_label.setText(os.path.basename(filepath))
            self.excel_conv_status_label.setText("")

    def _start_excel_to_pdf_conversion(self):
        if not self.excel_conv_path:
            QMessageBox.warning(self, "خطأ", "يرجى اختيار ملف Excel أولاً.")
            return

        output_path = os.path.splitext(self.excel_conv_path)[0] + ".pdf"
        self.excel_conv_status_label.setText("جاري التحويل إلى PDF...")
        worker = ExcelToPdfWorker(self.excel_conv_path, output_path)
        worker.signals.conversion_finished.connect(lambda msg: self.excel_conv_status_label.setText(msg))
        self.thread_pool.start(worker)

    def _create_video_merger_section(self, layout):
        self.vid_merge_video_path = ""
        self.vid_merge_audio_path = ""

        # Video selection
        h_layout1 = QHBoxLayout()
        self.vid_merge_select_video_btn = QPushButton("📂 اختر ملف الفيديو")
        self.vid_merge_video_label = QLabel("لم يتم اختيار ملف")
        h_layout1.addWidget(self.vid_merge_select_video_btn)
        h_layout1.addWidget(self.vid_merge_video_label, 1)
        layout.addLayout(h_layout1)

        # Audio selection
        h_layout2 = QHBoxLayout()
        self.vid_merge_select_audio_btn = QPushButton("🎵 اختر ملف الصوت")
        self.vid_merge_audio_label = QLabel("لم يتم اختيار ملف")
        h_layout2.addWidget(self.vid_merge_select_audio_btn)
        h_layout2.addWidget(self.vid_merge_audio_label, 1)
        layout.addLayout(h_layout2)

        self.vid_merge_start_btn = QPushButton("🔗 ابدأ الدمج")
        self.vid_merge_status_label = QLabel("")
        self.vid_merge_status_label.setWordWrap(True)
        layout.addWidget(self.vid_merge_start_btn, 0, Qt.AlignCenter)
        layout.addWidget(self.vid_merge_status_label)

        self.vid_merge_select_video_btn.clicked.connect(self._select_video_for_merge)
        self.vid_merge_select_audio_btn.clicked.connect(self._select_audio_for_merge)
        self.vid_merge_start_btn.clicked.connect(self._start_merge)

    def _select_video_for_merge(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر ملف الفيديو", "", "Video files (*.mp4 *.mkv *.webm *.avi *.mov)")
        if filepath:
            self.vid_merge_video_path = filepath
            self.vid_merge_video_label.setText(os.path.basename(filepath))
            self.vid_merge_status_label.setText("")

    def _select_audio_for_merge(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر ملف الصوت", "", "Audio files (*.mp3 *.m4a *.aac *.ogg *.wav)")
        if filepath:
            self.vid_merge_audio_path = filepath
            self.vid_merge_audio_label.setText(os.path.basename(filepath))
            self.vid_merge_status_label.setText("")

    def _start_merge(self):
        if not self.vid_merge_video_path or not self.vid_merge_audio_path:
            QMessageBox.warning(self, "خطأ", "يرجى اختيار ملف الفيديو وملف الصوت أولاً.")
            return

        output_folder = os.path.join(self.download_folder, "merged_videos")
        os.makedirs(output_folder, exist_ok=True)
        
        base_name, ext = os.path.splitext(os.path.basename(self.vid_merge_video_path))
        output_path = os.path.join(output_folder, f"{base_name}_merged{ext}")
        
        # Handle file name collisions
        counter = 1
        while os.path.exists(output_path):
            output_path = os.path.join(output_folder, f"{base_name}_merged ({counter}){ext}")
            counter += 1

        self.vid_merge_status_label.setText("جاري الدمج...")
        worker = VideoMergerWorker(self.vid_merge_video_path, self.vid_merge_audio_path, output_path)
        worker.signals.merge_finished.connect(lambda msg: self.vid_merge_status_label.setText(msg))
        self.thread_pool.start(worker)

    def _create_file_repair_section(self, layout):
        self.file_repair_path = ""

        title = QLabel("إصلاح ملف فيديو/صوت")
        title.setStyleSheet("font-size: 16px; font-weight: bold;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        desc = QLabel("هذه الأداة تحاول إصلاح أو استكمال الملفات غير المكتملة.\n"
                      "إذا كان الملف هو تحميل غير مكتمل من هذا البرنامج، سيتم سؤالك لاستكماله.")
        desc.setWordWrap(True)
        desc.setStyleSheet("font-size: 12px; color: #cccccc;")
        layout.addWidget(desc)

        h_layout = QHBoxLayout()
        self.repair_select_btn = QPushButton("📂 اختر ملف تالف")
        self.repair_path_label = QLabel("لم يتم اختيار ملف")
        self.combine_parts_btn = QPushButton("🔗 دمج أجزاء تحميل")
        self.combine_parts_btn.setToolTip("اختر أول جزء (مثلاً file.part0) لدمج كل الأجزاء في ملف واحد.")
        self.combine_parts_btn.setStyleSheet("background-color: #8e44ad;")

        h_layout.addWidget(self.repair_select_btn)
        h_layout.addWidget(self.repair_path_label, 1)
        h_layout.addSpacing(20)
        h_layout.addWidget(self.combine_parts_btn)
        layout.addLayout(h_layout)

        # Add the checkbox for deep repair
        self.deep_repair_checkbox = QCheckBox("محاولة إصلاح عميق (أبطأ، لكنه أكثر فعالية للملفات التي لا تعمل)")
        self.deep_repair_checkbox.setToolTip("يقوم هذا الخيار بإعادة ضغط الفيديو بالكامل، مما قد يصلح الأخطاء داخل الفيديو نفسه، ولكنه يستغرق وقتاً أطول.")
        layout.addWidget(self.deep_repair_checkbox)
        
        # --- FIX: Add a progress bar for the repair process ---
        self.repair_progress_bar = QProgressBar()
        self.repair_progress_bar.setRange(0, 100)
        self.repair_progress_bar.setValue(0)
        self.repair_progress_bar.setTextVisible(False)
        self.repair_progress_bar.hide() # Hide it initially
        layout.addWidget(self.repair_progress_bar)

        self.repair_start_btn = QPushButton("🛠️ ابدأ الإصلاح")
        self.repair_status_label = QLabel("")
        self.repair_status_label.setWordWrap(True)
        layout.addWidget(self.repair_start_btn, 0, Qt.AlignCenter) # Keep button centered
        layout.addWidget(self.repair_status_label)

        self.repair_select_btn.clicked.connect(self._select_file_for_repair)
        self.repair_start_btn.clicked.connect(self._start_repair)
        self.combine_parts_btn.clicked.connect(self._start_manual_combine)


    def _select_file_for_repair(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر ملفاً للإصلاح", "", "All Files (*.*)")
        if filepath:
            self.file_repair_path = filepath
            self.repair_path_label.setText(os.path.basename(filepath))
            self.repair_status_label.setText("")
            self.repair_progress_bar.hide()

    def _start_manual_combine(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "اختر أول جزء للدمج (file.part0)", "", "Part files (*.part0)")
        if not filepath:
            return

        # Check if it's actually the first part
        if not filepath.lower().endswith(".part0"):
            QMessageBox.warning(self, "خطأ", "يرجى اختيار أول ملف في السلسلة (الذي ينتهي بـ .part0).")
            return

        self.repair_progress_bar.hide()
        self.repair_status_label.setText("جاري دمج الأجزاء يدوياً...")
        worker = FileCombinerWorker(filepath)
        # --- FIX: Connect progress signal for repair tool ---
        worker.signals.merge_finished.connect(lambda msg: self.repair_status_label.setText(msg))
        self.thread_pool.start(worker)

    def _start_repair(self):
        if not self.file_repair_path:
            QMessageBox.warning(self, "خطأ", "يرجى اختيار ملف أولاً.")
            return

        self.repair_progress_bar.setValue(0)
        self.repair_progress_bar.hide()

        # Step 1: Check if it's a known incomplete download
        state = load_state()
        normalized_path = os.path.normpath(self.file_repair_path)
        task_data = state.get(normalized_path)

        if task_data:
            task_type = task_data.get("type", "direct") # Default to 'direct' for old states
            if task_type == "telethon":
                resume_message = "يبدو أن هذا الملف هو تحميل غير مكتمل من تيليجرام.\nهل تريد إضافته إلى قائمة التحميلات لمحاولة استكماله؟"
            else: # direct download
                resume_message = "يبدو أن هذا الملف هو تحميل غير مكتمل من البرنامج.\nهل تريد إضافته إلى قائمة التحميلات لاستكماله؟"

            reply = QMessageBox.question(self, "تحميل غير مكتمل", resume_message, QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if reply == QMessageBox.Yes:
                self._resume_download_from_repair(normalized_path, task_data)
            return

        # Step 2: If not a known download, ask the user what to do.
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("ملف غير معروف")
        msg_box.setText("لم يتم العثور على هذا الملف في سجل التحميلات.\nماذا تريد أن تفعل؟")
        msg_box.setIcon(QMessageBox.Question)
        complete_button = msg_box.addButton("📝 استكمال من رابط", QMessageBox.ActionRole)
        repair_button = msg_box.addButton("🛠️ محاولة الإصلاح (ffmpeg)", QMessageBox.ActionRole)
        cancel_button = msg_box.addButton("إلغاء", QMessageBox.RejectRole)
        msg_box.exec()

        clicked_button = msg_box.clickedButton()

        if clicked_button == complete_button:
            url, ok = QInputDialog.getText(self, "إدخال الرابط", "الرجاء إدخال رابط التحميل الأصلي للملف:", QLineEdit.Normal, "")
            if ok and url:
                self._start_file_completion(self.file_repair_path, url)
            else:
                self.repair_status_label.setText("تم إلغاء عملية الاستكمال.")
        elif clicked_button == repair_button:
            self._start_ffmpeg_repair()
        else:
            self.repair_status_label.setText("")

    def _start_file_completion(self, filepath, url):
        # --- NEW: Enhanced logic for handling all Telegram links ---
        if 't.me/' in url:
            tg_settings = self.settings.get("telegram", {})
            if not all(tg_settings.get(k) for k in ["api_id", "api_hash", "phone"]):
                QMessageBox.warning(self, "إعدادات ناقصة", "يرجى إعداد بيانات تيليجرام في صفحة الإعدادات أولاً.")
                self.repair_status_label.setText("فشل: إعدادات تيليجرام غير مكتملة.")
                return

            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')

            # Case 1: Direct post link (e.g., t.me/channel/123)
            if len(path_parts) == 2 and path_parts[1].isdigit():
                self.repair_status_label.setText("جاري جلب معلومات المنشور المباشر من تيليجرام...")
                # This worker is designed to fetch a single post directly.
                fetcher = TelethonDirectFetcher(
                    url, tg_settings["api_id"], tg_settings["api_hash"],
                    tg_settings.get("phone"), tg_settings.get("session_string")
                )
                # When details are fetched, it will call the function to create the download task.
                fetcher.signals.details_fetched.connect(self._create_telethon_task_from_repair_details)
                fetcher.signals.fetch_error.connect(lambda err: self.repair_status_label.setText(f"فشل جلب المنشور: {err}"))
                self.thread_pool.start(fetcher)

            # Case 2: Channel link (e.g., t.me/channel)
            else:
                self.repair_status_label.setText("جاري البحث في قناة تيليجرام عن الملف...")
                # This worker searches the whole channel.
                fetcher = TelethonFetcher(
                    url,
                    tg_settings["api_id"],
                    tg_settings["api_hash"],
                    tg_settings.get("phone"),
                    tg_settings.get("session_string"),
                    for_repair=True # Set the special flag
                )
                fetcher.signals.playlist_fetched_for_repair.connect(self._handle_telegram_channel_search)
                fetcher.signals.fetch_error.connect(lambda err: self.repair_status_label.setText(f"فشل البحث: {err}"))
                self.thread_pool.start(fetcher)
        else:
            # It's a direct link, proceed as before
            self.repair_status_label.setText("جاري التحضير لاستكمال الملف من الرابط المباشر...")
            worker = FileCompletionWorker(filepath, url)
            # Connect progress signal for completion
            worker.signals.progress.connect(self.on_repair_progress)
            worker.signals.merge_finished.connect(self._on_repair_finished)
            self.thread_pool.start(worker)

    def _on_repair_finished(self, message):
        """Slot to handle the final message from repair/completion workers."""
        self.repair_status_label.setText(message)
        self.repair_progress_bar.hide()

    @Slot(int, int, str)
    def on_repair_progress(self, task_id, percent, text):
        # Check for the special task_id (-2) to ensure this is from the repair worker
        if task_id == -2:
            if not self.repair_progress_bar.isVisible():
                self.repair_progress_bar.show()
            
            self.repair_progress_bar.setValue(percent)
            self.repair_status_label.setText(text)


    def _start_ffmpeg_repair(self):
        deep_repair = self.deep_repair_checkbox.isChecked()
        if deep_repair:
            self.repair_status_label.setText("جاري محاولة الإصلاح العميق (قد يستغرق بعض الوقت)...")
        else:
            self.repair_status_label.setText("جاري محاولة الإصلاح السريع باستخدام ffmpeg...")

        self.repair_progress_bar.setValue(0)
        self.repair_progress_bar.show()

        output_folder = os.path.join(self.download_folder, "repaired_files")
        os.makedirs(output_folder, exist_ok=True)
        base_name, ext = os.path.splitext(os.path.basename(self.file_repair_path))
        output_path = os.path.join(output_folder, f"{base_name}_repaired{ext}")

        worker = FileRepairWorker(self.file_repair_path, output_path, deep_repair=deep_repair)
        worker.signals.merge_finished.connect(lambda msg: self.repair_status_label.setText(msg))
        worker.signals.progress.connect(self.on_repair_progress)
        self.thread_pool.start(worker)

    def _resume_download_from_repair(self, filepath, data):
        task_type = data.get("type", "direct")

        if task_type == "direct":
            self._create_resumable_task_from_state(filepath, data)
        elif task_type == "telethon":
            # Re-create the telethon download task
            url = data['url']
            filename = data['filename']
            tg_settings = self.settings.get("telegram", {})
            self.last_task_id += 1
            task_id = self.last_task_id
            worker = TelethonDownloadWorker(task_id, url, self.download_folder, filename, tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
            widget = DownloadTaskWidget(filename, task_id, worker)
            self._setup_and_queue_worker(worker, widget)

        self.tabs.setCurrentWidget(self.downloader_page)
        QMessageBox.information(self, "تمت الإضافة", "تمت إضافة التحميل غير المكتمل إلى قائمة التحميلات.\nيمكنك الآن استئنافه من هناك.")

    @Slot(str, dict)
    def _handle_telegram_channel_search(self, url, playlist_info):
        """
        Callback for when TelethonFetcher finishes searching a channel for the repair tool.
        """
        local_filename = os.path.basename(self.file_repair_path)
        entries = playlist_info.get('entries', [])

        found_entry = None
        for entry in entries:
            # Compare the local filename with the filename from the Telegram entry
            # --- FIX: Use a safer, case-insensitive comparison ---
            if entry.get('filename') and entry.get('filename').lower() == local_filename.lower():
                found_entry = entry
                break

        if found_entry:
            self.repair_status_label.setText(f"✅ تم العثور على الملف! جاري إضافته لقائمة التحميلات للاستكمال...")
            # We have the entry, now create a download worker for it.
            # --- FIX: Use a more direct method to create the task ---
            self._create_telethon_task_from_repair(found_entry)
            # Switch to the downloader tab to see the new task
            self.tabs.setCurrentWidget(self.downloader_page)
            QMessageBox.information(self, "تم العثور على الملف", "تم العثور على الملف في القناة وإضافته إلى قائمة التحميلات.\nسيتم الآن استكمال تحميله.")
        else:
            # --- NEW: If no match is found, show a selection dialog ---
            self.repair_status_label.setText(f"لم يتم العثور على تطابق تلقائي. يرجى الاختيار يدوياً.")
            dialog = PlaylistSelectionDialog(playlist_info, self)
            dialog.setWindowTitle("اختر الملف الصحيح للاستكمال")
            # Connect the selection to our new handler
            dialog.video_selected.connect(self._create_telethon_task_from_repair_by_url)
            if not dialog.exec():
                self.repair_status_label.setText("تم إلغاء الاختيار اليدوي.")

    @Slot(str, str, object)
    def _create_telethon_task_from_repair_details(self, internal_url, filename, total_size):
        """
        Creates a Telethon download task after details have been fetched directly.
        This is the callback for TelethonDirectFetcher in the repair tool.
        """
        local_filename = os.path.basename(self.file_repair_path)
        if filename.lower() != local_filename.lower():
            reply = QMessageBox.question(self, "تحذير عدم تطابق",
                                         f"الملف الذي اخترته ({local_filename}) يختلف عن اسم الملف في الرابط ({filename}).\n"
                                         "هل أنت متأكد أنك تريد استكمال هذا الملف؟",
                                         QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply == QMessageBox.No:
                self.repair_status_label.setText("تم الإلغاء بسبب عدم تطابق الأسماء.")
                return

        self._create_telethon_task_from_repair({'url': internal_url, 'filename': filename})

    @Slot(str)
    def on_ffmpeg_progress(self, message):
        if hasattr(self, 'ffmpeg_install_button'):
            self.ffmpeg_install_button.setText(message)

    @Slot(bool, str)
    def on_ffmpeg_finished(self, success, message):
        global FFMPEG_AVAILABLE
        if success:
            FFMPEG_AVAILABLE = True
            self._update_merger_ui()
            self.vid_merge_status_label.setText(message)
            if hasattr(self, 'ffmpeg_install_button'):
                self._update_repair_ui() # Also update repair UI
                self.ffmpeg_install_button.setEnabled(True)
                self.ffmpeg_install_button.setText("❌ فشل التثبيت. اضغط للمحاولة مرة أخرى.")
            QMessageBox.critical(self, "فشل تثبيت FFMpeg", message)

    def _create_telethon_task_from_repair(self, entry):
        """Helper to create a Telethon download task from a repair entry."""
        video_url = entry['url']
        raw_filename = entry.get('filename', f"telegram_file_{self.last_task_id + 1}")
        filename = self._sanitize_filename(raw_filename)

        self.last_task_id += 1
        task_id = self.last_task_id
        tg_settings = self.settings.get("telegram", {})

        worker = TelethonDownloadWorker(task_id, video_url, self.download_folder, filename, tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
        widget = DownloadTaskWidget(filename, task_id, worker)
        self._setup_and_queue_worker(worker, widget)

    def _create_telethon_task_from_repair_by_url(self, video_url):
        """Finds the entry corresponding to the URL and creates a task."""
        # Find the dialog that is still open
        dialog = self.findChild(PlaylistSelectionDialog)
        if not dialog: return

        selected_entry = next((e for e in dialog.entries if e['url'] == video_url), None)
        if selected_entry:
            self._create_telethon_task_from_repair(selected_entry)
            self.tabs.setCurrentWidget(self.downloader_page)

    def load_settings(self):
        """Loads settings from settings.json."""
        try:
            with open("settings.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"download_folder": "downloads"}

    def save_settings(self):
        """Saves current settings to settings.json."""
        with open("settings.json", "w", encoding="utf-8") as f:
            # التأكد من حفظ القيمة المحدثة من واجهة المستخدم
            self.settings['max_concurrent_downloads'] = self.max_downloads_spinbox.value()
            json.dump(self.settings, f, ensure_ascii=False, indent=4)

    def _create_settings_ui(self, parent_widget):
        """Creates all UI elements for the settings page."""
        # --- الحل: جعل صفحة الإعدادات قابلة للتمرير ---
        # 1. ننشئ حاوية رئيسية للتمرير ونضيفها إلى الصفحة.
        scroll_area = QScrollArea(parent_widget)
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("QScrollArea { border: none; }") # لإزالة أي حدود

        # 2. ننشئ ويدجت داخلية لتكون هي المحتوى القابل للتمرير.
        settings_content_widget = QWidget()
        layout = QVBoxLayout(settings_content_widget) # كل العناصر ستوضع في هذا التنسيق
        layout.setAlignment(Qt.AlignTop) # لضمان بدء المحتوى من الأعلى
        scroll_area.setWidget(settings_content_widget)

        title = QLabel("إعدادات البرنامج")
        title.setStyleSheet("font-size: 18px; font-weight: bold; padding-bottom: 10px;")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title, 0, Qt.AlignTop)

        # Download Folder Setting
        folder_layout = QHBoxLayout()
        folder_label = QLabel("مجلد التحميلات الافتراضي:")
        self.settings_folder_input = QLineEdit(self.download_folder)
        self.settings_folder_input.setReadOnly(True)
        change_folder_btn = QPushButton("تغيير...")
        
        folder_layout.addWidget(folder_label)
        folder_layout.addWidget(self.settings_folder_input, 1)
        folder_layout.addWidget(change_folder_btn)
        layout.addLayout(folder_layout)
        change_folder_btn.clicked.connect(self._change_download_folder)

        # --- Add a spacer ---
        layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # --- Max Concurrent Downloads Setting ---
        max_downloads_layout = QHBoxLayout()
        max_downloads_label = QLabel("الحد الأقصى للتحميلات المتزامنة:")
        self.max_downloads_spinbox = QSpinBox()
        self.max_downloads_spinbox.setRange(1, 10) # تحديد نطاق من 1 إلى 10
        self.max_downloads_spinbox.setValue(self.MAX_CONCURRENT_DOWNLOADS)
        self.max_downloads_spinbox.valueChanged.connect(self._update_max_downloads)

        max_downloads_layout.addWidget(max_downloads_label)
        max_downloads_layout.addWidget(self.max_downloads_spinbox)
        max_downloads_layout.addStretch()
        layout.addLayout(max_downloads_layout)
        # --- Add a spacer ---
        layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # --- yt-dlp Updater ---
        if YTDLP_AVAILABLE:
            ytdlp_layout = QHBoxLayout()
            ytdlp_desc = QLabel("تحديث مكتبة يوتيوب (yt-dlp) يحل مشاكل التحميل من يوتيوب والمواقع الأخرى.")
            ytdlp_desc.setWordWrap(True)
            self.update_ytdlp_button = QPushButton("🔄 تحديث yt-dlp")
            self.update_ytdlp_button.setStyleSheet("background-color: #e67e22;")
            self.update_ytdlp_button.clicked.connect(self._start_ytdlp_update)

            ytdlp_layout.addWidget(ytdlp_desc, 1)
            ytdlp_layout.addWidget(self.update_ytdlp_button)
            layout.addLayout(ytdlp_layout)

            self.ytdlp_status_label = QLabel("")
            layout.addWidget(self.ytdlp_status_label)

            # --- Add another spacer ---
            layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))


        # --- Protocol Handler Setting ---
        protocol_title = QLabel("التكامل مع النظام")
        protocol_title.setStyleSheet("font-size: 16px; font-weight: bold;")
        layout.addWidget(protocol_title)

        protocol_desc = QLabel("تسجيل بروتوكول 'mostafa-dl://' يتيح للبرنامج فتح روابط التحميل مباشرة من المتصفح.")
        protocol_desc.setWordWrap(True)
        layout.addWidget(protocol_desc)

        # --- حاوية لأزرار البروتوكول لتسهيل التبديل بينها ---
        self.protocol_buttons_layout = QHBoxLayout()
        self.register_protocol_button = QPushButton("🔗 تسجيل بروتوكول mostafa-dl://")
        self.register_protocol_button.setStyleSheet("background-color: #8e44ad;")
        self.register_protocol_button.clicked.connect(self._register_protocol_handler)

        self.unregister_protocol_button = QPushButton("🗑️ إلغاء تسجيل البروتوكول")
        self.unregister_protocol_button.setStyleSheet("background-color: #c0392b;")
        self.unregister_protocol_button.clicked.connect(self._unregister_protocol_handler)

        self.protocol_buttons_layout.addWidget(self.register_protocol_button)
        self.protocol_buttons_layout.addWidget(self.unregister_protocol_button)
        self.protocol_buttons_layout.addStretch()
        layout.addLayout(self.protocol_buttons_layout)
        self._update_protocol_button_visibility() # Check status on startup

        # --- Browser Integration (Native Messaging) ---
        self.browser_buttons_layout = QHBoxLayout()
        self.register_browser_button = QPushButton("🔗 ربط البرنامج مع متصفح جوجل كروم")
        self.register_browser_button.setStyleSheet("background-color: #27ae60;")
        self.register_browser_button.setToolTip("يقوم هذا الزر بتسجيل البرنامج في سجل الويندوز لتمكين إضافة المتصفح من التواصل معه.")
        self.register_browser_button.clicked.connect(self._register_native_host)

        self.unregister_browser_button = QPushButton("🗑️ إلغاء ربط المتصفح")
        self.unregister_browser_button.setStyleSheet("background-color: #c0392b;")
        self.unregister_browser_button.clicked.connect(self._unregister_native_host)
        self.browser_buttons_layout.addWidget(self.register_browser_button)
        self.browser_buttons_layout.addWidget(self.unregister_browser_button)
        self.browser_buttons_layout.addStretch()
        layout.addLayout(self.browser_buttons_layout)
        self._update_browser_button_visibility()


        # --- Add another spacer ---
        layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # --- Add another spacer ---
        layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Fixed))

        # --- Telegram (Telethon) Settings ---
        if TELETHON_AVAILABLE:
            telegram_title = QLabel("إعدادات تيليجرام (للقنوات الخاصة)")
            telegram_title.setStyleSheet("font-size: 16px; font-weight: bold;")
            layout.addWidget(telegram_title)

            telegram_desc = QLabel("أدخل بيانات API الخاصة بك للوصول إلى القنوات الخاصة أو التي تحتاج إلى تسجيل دخول.")
            telegram_desc.setWordWrap(True)
            layout.addWidget(telegram_desc)

            self._create_telegram_settings_section(layout)

        # Add a spacer at the bottom to push everything up
        layout.addStretch()

        # 3. نضيف حاوية التمرير إلى التنسيق الرئيسي لصفحة الإعدادات.
        parent_layout = QVBoxLayout(parent_widget)
        parent_layout.setContentsMargins(0, 0, 0, 0)
        parent_layout.addWidget(scroll_area)

    def _update_repair_ui(self):
        """Clears and repopulates the file repair section based on FFMPEG availability."""
        # Clear previous widgets
        while self.file_repair_layout.count():
            child = self.file_repair_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        global FFMPEG_AVAILABLE
        FFMPEG_AVAILABLE = shutil.which('ffmpeg') is not None

        if FFMPEG_AVAILABLE:
            self._create_file_repair_section(self.file_repair_layout)
        # If FFMPEG is not available, the merger UI will show the install button, so we do nothing here.

    def _register_protocol_handler(self):
        self._run_registry_operation(self._perform_registration)

    def _unregister_protocol_handler(self):
        self._run_registry_operation(self._perform_unregistration)

    def _run_registry_operation(self, operation_func):
        """
        A helper function to run a registry operation, handling platform checks,
        confirmation dialogs, and exceptions for both registration and unregistration.
        """
        if sys.platform != "win32":
            QMessageBox.information(self, "غير مدعوم", "هذه الميزة متاحة حالياً على نظام ويندوز فقط.")
            return

        if sys.platform != "win32":
            QMessageBox.information(self, "غير مدعوم", "هذه الميزة متاحة حالياً على نظام ويندوز فقط.")
            return

        reply = QMessageBox.question(self, "تأكيد التسجيل",
                                     "سيقوم هذا الإجراء بتعديل سجل الويندوز (Registry) لجعل هذا البرنامج يتعامل مع روابط التحميل الخاصة.\n\n"
                                     "هذا يتطلب صلاحيات المسؤول. هل تريد المتابعة؟",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No: return
        
        try:
            operation_func()
        except PermissionError:
            QMessageBox.critical(self, "خطأ في الصلاحيات", "فشل تعديل سجل الويندوز.\nيرجى إعادة تشغيل البرنامج 'كمسؤول' (Run as administrator) والمحاولة مرة أخرى.")
        except Exception as e:
            QMessageBox.critical(self, "خطأ", f"حدث خطأ غير متوقع: {e}")
        finally:
            # Update button visibility after any attempt
            self._update_protocol_button_visibility()

    def _perform_registration(self):
        """The actual logic for writing to the registry."""
        import winreg
        command = f'"{sys.executable}" "{os.path.abspath(sys.argv[0])}" "%1"'
        protocol_name = "mostafa-dl"

        key = winreg.CreateKey(winreg.HKEY_CLASSES_ROOT, protocol_name)
        winreg.SetValue(key, None, winreg.REG_SZ, "URL:Mostafa Downloader Protocol")
        winreg.SetValueEx(key, "URL Protocol", 0, winreg.REG_SZ, "")
        winreg.CloseKey(key)

        key = winreg.CreateKey(winreg.HKEY_CLASSES_ROOT, f'{protocol_name}\\shell\\open\\command')
        winreg.SetValue(key, None, winreg.REG_SZ, command)
        winreg.CloseKey(key)
        QMessageBox.information(self, "نجاح", "تم تسجيل البروتوكول بنجاح!\n\nمثال: mostafa-dl://https://example.com/file.zip")

    def _perform_unregistration(self):
        """The actual logic for deleting from the registry."""
        import winreg
        protocol_name = "mostafa-dl"
        try:
            # Deleting a key with subkeys requires recursion or a special function.
            # winreg.DeleteKey doesn't work on non-empty keys. We'll delete subkeys first.
            winreg.DeleteKey(winreg.HKEY_CLASSES_ROOT, f'{protocol_name}\\shell\\open\\command')
            winreg.DeleteKey(winreg.HKEY_CLASSES_ROOT, f'{protocol_name}\\shell\\open')
            winreg.DeleteKey(winreg.HKEY_CLASSES_ROOT, f'{protocol_name}\\shell')
            winreg.DeleteKey(winreg.HKEY_CLASSES_ROOT, protocol_name)
            QMessageBox.information(self, "نجاح", "تم إلغاء تسجيل البروتوكول بنجاح.")
        except FileNotFoundError:
            QMessageBox.information(self, "معلومة", "البروتوكول غير مسجل بالفعل.")

    def _update_protocol_button_visibility(self):
        """Checks if the protocol is registered and shows the appropriate button."""

        if sys.platform != "win32":
            self.register_protocol_button.show()
            self.unregister_protocol_button.hide()
            return
        
        import winreg
        try:
            key = winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, "mostafa-dl")
            winreg.CloseKey(key)
            # If OpenKey succeeds, the key exists.
            self.register_protocol_button.hide()
            self.unregister_protocol_button.show()
        except FileNotFoundError:
            # If it fails, the key doesn't exist.
            self.register_protocol_button.show()
            self.unregister_protocol_button.hide()

    def _unregister_native_host(self):
        self._run_registry_operation(self._perform_native_host_unregistration)

    def _register_native_host(self):
        """
        Registers the native messaging host manifest file in the Windows Registry
        so that the Chrome extension can find and communicate with it.
        """
        if sys.platform != "win32":
            QMessageBox.information(self, "غير مدعوم", "هذه الميزة متاحة حالياً على نظام ويندوز فقط.")
            return

        reply = QMessageBox.question(self, "تأكيد ربط المتصفح",
                                     "سيقوم هذا الإجراء بتعديل سجل الويندوز (Registry) لربط إضافة المتصفح بهذا البرنامج.\n\n"
                                     "هذا يتطلب صلاحيات المسؤول. هل تريد المتابعة؟",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No: return

        try:
            import winreg
            host_name = "com.mostafa.downloader"
            manifest_path = os.path.abspath(f"{host_name}.json")

            if not os.path.exists(manifest_path):
                QMessageBox.critical(self, "خطأ", f"ملف الربط '{manifest_path}' غير موجود. لا يمكن المتابعة.")
                return

            # The key where Chrome looks for native messaging hosts
            key_path = f"SOFTWARE\\Google\\Chrome\\NativeMessagingHosts\\{host_name}"
            with winreg.CreateKey(winreg.HKEY_CURRENT_USER, key_path) as key:
                winreg.SetValue(key, None, winreg.REG_SZ, manifest_path)
            QMessageBox.information(self, "نجاح", "تم ربط البرنامج مع جوجل كروم بنجاح!\n\nأعد تشغيل المتصفح لتفعيل التغييرات.")
        except PermissionError:
            QMessageBox.critical(self, "خطأ في الصلاحيات", "فشل تعديل سجل الويندوز.\nيرجى إعادة تشغيل البرنامج 'كمسؤول' (Run as administrator) والمحاولة مرة أخرى.")
        except Exception as e:
            QMessageBox.critical(self, "خطأ", f"حدث خطأ غير متوقع أثناء التسجيل: {e}")
        finally:
            self._update_browser_button_visibility()

    def _perform_native_host_unregistration(self):
        """Deletes the native messaging host key from the registry."""
        import winreg
        host_name = "com.mostafa.downloader"
        key_path = f"SOFTWARE\\Google\\Chrome\\NativeMessagingHosts\\{host_name}"
        try:
            winreg.DeleteKey(winreg.HKEY_CURRENT_USER, key_path)
            QMessageBox.information(self, "نجاح", "تم إلغاء ربط البرنامج مع جوجل كروم بنجاح.")
        except FileNotFoundError:
            QMessageBox.information(self, "معلومة", "البرنامج غير مربوط بالمتصفح بالفعل.")

    def _update_browser_button_visibility(self):
        """Checks if the native host is registered and shows the appropriate button."""
        if sys.platform != "win32":
            self.register_browser_button.show()
            self.unregister_browser_button.hide()
            return

        import winreg
        host_name = "com.mostafa.downloader"
        key_path = f"SOFTWARE\\Google\\Chrome\\NativeMessagingHosts\\{host_name}"
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path)
            winreg.CloseKey(key)
            # If OpenKey succeeds, the key exists.
            self.register_browser_button.hide()
            self.unregister_browser_button.show()
        except FileNotFoundError:
            # If it fails, the key doesn't exist.
            self.register_browser_button.show()
            self.unregister_browser_button.hide()

    def _change_download_folder(self):
        """Opens a dialog to select a new download folder."""
        new_folder = QFileDialog.getExistingDirectory(self, "اختر مجلد التحميلات الجديد", self.download_folder)
        if new_folder and new_folder != self.download_folder:
            self.download_folder = new_folder
            self.settings['download_folder'] = new_folder
            self.save_settings()
            self.settings_folder_input.setText(new_folder)
            os.makedirs(new_folder, exist_ok=True)
            QMessageBox.information(self, "تم الحفظ", "تم حفظ مجلد التحميلات الجديد بنجاح.")

    @Slot(int)
    def _update_max_downloads(self, value):
        """Applies the new max concurrent downloads value immediately."""
        self.MAX_CONCURRENT_DOWNLOADS = value
        self.save_settings() # Save the setting whenever it's changed
        self._start_next_in_queue() # Try to start new downloads if slots have opened up

    def _start_ytdlp_update(self):
        self.update_ytdlp_button.setEnabled(False)
        worker = YtdlpUpdaterWorker()
        worker.signals.ytdlp_updated.connect(self.on_ytdlp_updated)
        self.thread_pool.start(worker)

    @Slot(bool, str)
    def on_ytdlp_updated(self, success, message):
        self.ytdlp_status_label.setText(message)
        if success:
            self.ytdlp_status_label.setStyleSheet("color: lightgreen;")
        self.update_ytdlp_button.setEnabled(True)

    def _create_telegram_settings_section(self, layout):
        tg_settings = self.settings.get("telegram", {})

        # --- تعديل: استخدام قيم ثابتة وإخفاء الحقول من الواجهة ---
        self.TG_API_ID = "20961519"
        self.TG_API_HASH = "0d57a9b5a975c6770f0797b9ea75ebe6"

        # إنشاء الحقول ولكن إخفاؤها، قد نحتاجها داخلياً
        self.tg_api_id_input = QLineEdit(self.TG_API_ID)
        self.tg_api_hash_input = QLineEdit(self.TG_API_HASH)
        self.tg_api_id_input.hide()
        self.tg_api_hash_input.hide()

        self.tg_phone_input = QLineEdit(tg_settings.get("phone", ""))

        # --- تعديل: إخفاء حقل الهاتف وإضافة أزرار جديدة ---
        self.tg_phone_input.hide() # إخفاء حقل الهاتف

        # --- حاوية للأزرار لتسهيل إظهارها وإخفائها ---
        self.tg_buttons_container = QWidget()
        buttons_layout = QHBoxLayout(self.tg_buttons_container)
        buttons_layout.setContentsMargins(0,0,0,0)

        self.tg_qr_login_button = QPushButton("📱 تسجيل الدخول عبر QR Code")
        self.tg_qr_login_button.setStyleSheet("background-color: #2ecc71; color: black;") # لون أخضر مميز
        self.tg_qr_login_button.clicked.connect(self._start_qr_login)

        self.tg_manual_login_button = QPushButton("⌨️ تسجيل الدخول يدوياً")
        self.tg_manual_login_button.clicked.connect(self._save_and_login_telegram)

        self.tg_logout_button = QPushButton("🚪 تسجيل الخروج")
        self.tg_logout_button.setStyleSheet("background-color: #c0392b;") # لون أحمر
        self.tg_logout_button.clicked.connect(self._logout_telegram)

        buttons_layout.addWidget(self.tg_qr_login_button, 1) # The '1' makes it stretch
        buttons_layout.addWidget(self.tg_manual_login_button)
        buttons_layout.addWidget(self.tg_logout_button, 1)

        layout.addWidget(self.tg_buttons_container)

        self.tg_status_label = QLabel("الحالة: لم يتم تسجيل الدخول.")
        layout.addWidget(self.tg_status_label)

        # This will hold the worker so we can send data back to it
        self.telegram_login_worker = None
        self._update_telegram_ui_state() # تحديث الواجهة بناءً على حالة تسجيل الدخول

    def on_tab_changed(self, index):
        """Called when the user switches tabs."""
        # Check if the newly selected tab is the Telegram Manager
        if self.tabs.tabText(index) == "🗂️ مدير تيليجرام":
            # Automatically refresh the dialogs list if it's empty
            if self.tm_dialogs_tree.topLevelItemCount() == 0:
                self._fetch_telegram_dialogs()
    def _update_telegram_ui_state(self):
        """Updates the Telegram settings UI based on whether the user is logged in."""
        # --- FIX: Ensure is_logged_in is always a boolean ---
        # The original check could return the session string itself, causing a TypeError.
        # This ensures we get True if the string exists and is not empty, and False otherwise.
        is_logged_in = bool(self.settings.get("telegram", {}).get("session_string"))
        self.tg_qr_login_button.setVisible(not is_logged_in)
        self.tg_manual_login_button.setVisible(not is_logged_in)
        self.tg_logout_button.setVisible(is_logged_in)

        if is_logged_in:
            self.tg_status_label.setText("الحالة: تم تسجيل الدخول بنجاح.")
            self.tg_status_label.setStyleSheet("color: lightgreen;")
        else:
            self.tg_status_label.setText("الحالة: لم يتم تسجيل الدخول.")
            self.tg_status_label.setStyleSheet("color: '';") # Reset color
        
        # --- تحديث حالة صفحة مدير تيليجرام ---
        if hasattr(self, 'telegram_page'):
            is_enabled = bool(self.settings.get("telegram", {}).get("session_string"))
            self.telegram_page.setEnabled(is_enabled)
            self.tm_status_label.setText("يرجى تسجيل الدخول من صفحة الإعدادات أولاً." if not is_enabled else "جاهز. اضغط على 'تحديث' لبدء التصفح.")


    def _save_and_login_telegram(self):
        api_id = self.TG_API_ID
        api_hash = self.TG_API_HASH

        # --- تعديل: طلب رقم الهاتف عبر نافذة منبثقة ---
        phone, ok = QInputDialog.getText(self, 'تسجيل الدخول يدوياً', 'أدخل رقم الهاتف (بالصيغة الدولية مثل +20123456789):')

        # إذا ضغط المستخدم "Cancel" أو لم يدخل شيئاً، نوقف العملية
        if not ok or not phone:
            self.tg_status_label.setText("تم إلغاء تسجيل الدخول اليدوي.")
            self.tg_status_label.setStyleSheet("color: orange;")
            return

        # Save credentials first
        if "telegram" not in self.settings: self.settings["telegram"] = {}
        self.settings["telegram"]["api_id"] = api_id
        self.settings["telegram"]["api_hash"] = api_hash
        self.settings["telegram"]["phone"] = phone
        self.save_settings()

        self.tg_status_label.setText("جاري محاولة الاتصال...")
        QApplication.processEvents() # Update UI

        # The login logic is now in its own QRunnable class
        self.telegram_login_worker = TelegramLoginWorker(api_id, api_hash, phone)
        self.telegram_login_worker.signals.telegram_code_required.connect(self.on_telegram_code_required)
        self.telegram_login_worker.signals.telegram_password_required.connect(self.on_telegram_password_required)
        self.telegram_login_worker.signals.telegram_login_finished.connect(self.on_telegram_login_finished)
        self.telegram_login_worker.signals.telegram_qr_updated.connect(self.on_telegram_qr_updated) # Connect new signal

        self.thread_pool.start(self.telegram_login_worker)

    @Slot()
    def on_telegram_code_required(self):
        """Called from the worker thread via a signal to safely ask for the code in the main thread."""
        code, ok = QInputDialog.getText(self, 'رمز التحقق', 'أدخل رمز التحقق الذي وصلك على تيليجرام:')
        if ok and code and self.telegram_login_worker:
            self.telegram_login_worker.set_code(code)
        elif self.telegram_login_worker:
            self.telegram_login_worker.cancel() # User cancelled

    @Slot()
    def on_telegram_password_required(self):
        """Called from the worker thread via a signal to safely ask for the password in the main thread."""
        password, ok = QInputDialog.getText(self, 'كلمة المرور', 'أدخل كلمة مرور المصادقة الثنائية (2FA):', QLineEdit.Password)
        if ok and password and self.telegram_login_worker:
            self.telegram_login_worker.set_password(password)
        elif self.telegram_login_worker:
            self.telegram_login_worker.cancel() # User cancelled

    @Slot(bool, str)
    def on_telegram_login_finished(self, success, result):
        """Called when the login process is fully complete."""
        if success:
            session_string = result
            self.settings["telegram"]["session_string"] = session_string
            self.save_settings()
            self._update_telegram_ui_state() # تحديث الواجهة
        else:
            error_message = result
            self.tg_status_label.setText(f"فشل تسجيل الدخول: {error_message}")
            self.tg_status_label.setStyleSheet("color: red;")
            self._update_telegram_ui_state() # تحديث الواجهة
        
        self.telegram_login_worker = None # Clean up worker reference
        # Close the QR dialog if it's open
        if hasattr(self, 'qr_dialog') and self.qr_dialog.isVisible():
            self.qr_dialog.close()

    def _logout_telegram(self):
        """Logs the user out by clearing the session string."""
        if "telegram" in self.settings and "session_string" in self.settings["telegram"]:
            del self.settings["telegram"]["session_string"]
            self.save_settings()
            QMessageBox.information(self, "نجاح", "تم تسجيل الخروج من حساب تيليجرام بنجاح.")
            self._update_telegram_ui_state()
        else:
            QMessageBox.warning(self, "خطأ", "أنت غير مسجل الدخول بالفعل.")

    def _start_qr_login(self):
        """Starts the QR code login process."""
        if not QRCODE_AVAILABLE:
            QMessageBox.critical(self, "مكتبة ناقصة", "ميزة تسجيل الدخول عبر QR Code تتطلب تثبيت مكتبة `qrcode`.\n\nقم بتشغيل الأمر: pip install qrcode[pil]")
            return

        api_id = self.TG_API_ID
        api_hash = self.TG_API_HASH
        # Save credentials
        if "telegram" not in self.settings: self.settings["telegram"] = {}
        self.settings["telegram"]["api_id"] = api_id
        self.settings["telegram"]["api_hash"] = api_hash
        self.save_settings()

        self.tg_status_label.setText("في انتظار مسح QR Code...")
        QApplication.processEvents()

        # Create and show the QR dialog
        self.qr_dialog = QRLoginDialog(self)
        self.qr_dialog.rejected.connect(self._cancel_qr_login) # Handle user closing the dialog

        # Start the worker
        self.telegram_login_worker = TelegramQRLoginWorker(api_id, api_hash)
        self.telegram_login_worker.signals.telegram_qr_updated.connect(self.on_telegram_qr_updated)
        self.telegram_login_worker.signals.telegram_login_finished.connect(self.on_telegram_login_finished)
        self.thread_pool.start(self.telegram_login_worker)

        self.qr_dialog.exec()

    @Slot(QImage)
    def on_telegram_qr_updated(self, qr_image):
        """Updates the QR code image in the dialog."""
        if hasattr(self, 'qr_dialog') and self.qr_dialog.isVisible():
            self.qr_dialog.update_qr_code(qr_image)

    def _cancel_qr_login(self):
        """Called when the user closes the QR dialog."""
        if self.telegram_login_worker:
            self.telegram_login_worker.cancel()
            self.tg_status_label.setText("تم إلغاء تسجيل الدخول.")
            self.tg_status_label.setStyleSheet("color: orange;")

    # --- دوال وعوامل مدير تيليجرام الجديدة ---

    def _create_telegram_manager_ui(self, parent_widget):
        """Creates the UI for the Telegram Manager tab."""
        main_layout = QVBoxLayout(parent_widget)

        # --- Status and Refresh Button ---
        top_layout = QHBoxLayout()
        self.tm_status_label = QLabel("يرجى تسجيل الدخول من صفحة الإعدادات أولاً.")
        self.tm_refresh_button = QPushButton("🔄 تحديث القنوات")
        self.tm_refresh_button.clicked.connect(self._fetch_telegram_dialogs)
        self.tm_status_label.mouseDoubleClickEvent = self._show_rejected_dialogs
        top_layout.addWidget(self.tm_status_label, 1)
        top_layout.addWidget(self.tm_refresh_button)
        main_layout.addLayout(top_layout)

        # --- Main content area (Dialogs and Files) ---
        content_layout = QHBoxLayout()

        # --- Dialogs List (Left Side) ---
        dialogs_container = QWidget()
        dialogs_layout = QVBoxLayout(dialogs_container)
        dialogs_layout.addWidget(QLabel("القنوات والمجموعات:"))
        # --- إضافة شريط بحث للقنوات ---
        dialog_search_layout = QHBoxLayout()
        self.tm_dialog_search = QLineEdit()
        self.tm_dialog_search.setPlaceholderText("🔍 ابحث عن قناة...")
        self.tm_dialog_search.textChanged.connect(self._filter_tm_dialogs)
        self.tm_username_search_button = QPushButton("بحث بالمعرف")
        self.tm_username_search_button.setToolTip("ابحث عن قناة عامة أو خاصة باستخدام اسمها (username)")
        self.tm_username_search_button.clicked.connect(self._search_by_telegram_username)
        dialog_search_layout.addWidget(self.tm_dialog_search, 1)
        dialog_search_layout.addWidget(self.tm_username_search_button)
        dialogs_layout.addLayout(dialog_search_layout)

        self.tm_dialogs_tree = QTreeWidget()
        self.tm_dialogs_tree.setHeaderLabels(["الاسم"])
        self.tm_dialogs_tree.header().setSectionResizeMode(QHeaderView.Stretch)
        self.tm_dialogs_tree.setIconSize(QSize(48, 48)) # زيادة حجم الأيقونة
        self.tm_dialogs_tree.itemClicked.connect(self._on_dialog_selected)
        dialogs_layout.addWidget(self.tm_dialogs_tree)
        # --- جديد: تفعيل قائمة السياق المخصصة ---
        self.tm_dialogs_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tm_dialogs_tree.customContextMenuRequested.connect(self._show_dialog_context_menu)

        content_layout.addWidget(dialogs_container, 1) # 1/3 of the space

        # --- Files List (Right Side) ---
        files_container = QWidget()
        files_layout = QVBoxLayout(files_container)
        
        # --- شريط التحكم بالملفات (بحث وزر تحميل) ---
        files_controls_layout = QHBoxLayout()
        files_controls_layout.addWidget(QLabel("الملفات المتاحة:"))
        # --- جديد: زر التشغيل المباشر ---
        self.tm_play_selected_button = QPushButton("▶️ تشغيل")
        self.tm_play_selected_button.clicked.connect(self._play_selected_tm_file)
        files_controls_layout.addStretch()
        self.tm_download_selected_button = QPushButton("⬇️ تحميل المحدد")
        self.tm_download_selected_button.clicked.connect(self._download_selected_tm_files)
        files_controls_layout.addWidget(self.tm_play_selected_button)
        files_controls_layout.addWidget(self.tm_download_selected_button)
        files_layout.addLayout(files_controls_layout)

        # --- إضافة شريط بحث للملفات ---
        self.tm_file_search = QLineEdit()
        self.tm_file_search.setPlaceholderText("🔍 ابحث عن ملف...")
        self.tm_file_search.textChanged.connect(self._filter_tm_files)
        files_layout.addWidget(self.tm_file_search)

        # --- تعديل: استخدام QListWidget مع وضع الأيقونات لعرض الصور بشكل أفضل ---
        self.tm_files_list = QListWidget()
        self.tm_files_list.setViewMode(QListWidget.IconMode) # عرض العناصر كشبكة أيقونات
        self.tm_files_list.setResizeMode(QListWidget.Adjust) # تعديل الشبكة تلقائياً مع حجم النافذة
        self.tm_files_list.setMovement(QListWidget.Static) # منع تحريك العناصر
        self.tm_files_list.setSpacing(15) # زيادة المسافة بين العناصر
        self.tm_files_list.setIconSize(QSize(160, 160)) # حجم كبير وواضح للصور المصغرة
        self.tm_files_list.setWordWrap(True) # التفاف النص لأسماء الملفات الطويلة
        self.tm_files_list.itemDoubleClicked.connect(self._on_file_double_clicked)
        files_layout.addWidget(self.tm_files_list)

        content_layout.addWidget(files_container, 2) # 2/3 of the space
        
        # --- تفعيل التحديد المتعدد في قائمة الملفات ---
        self.tm_files_list.setSelectionMode(QAbstractItemView.ExtendedSelection)

        # --- جديد: تخزين قائمة الملفات الكاملة للفلترة ---
        self.tm_all_files_cache = []
        self._create_tm_filter_buttons(files_layout)

        # --- جديد: تتبع عامل جلب الملفات النشط لإلغائه عند الحاجة ---
        self.active_files_fetcher = None

        main_layout.addLayout(content_layout)
        self._update_telegram_ui_state() # Set initial enabled/disabled state

    def _show_dialog_context_menu(self, position):
        """يُظهر قائمة السياق عند النقر بالزر الأيمن على عنصر في شجرة القنوات."""
        item = self.tm_dialogs_tree.itemAt(position)
        if not item:
            return

        dialog_id = item.data(0, Qt.UserRole)
        dialog_info = item.data(1, Qt.UserRole) # استرداد كامل معلومات القناة
        if not dialog_id or not dialog_info:
            return

        menu = QMenu(self)

        # --- 1. خيار مغادرة القناة ---
        leave_action = menu.addAction("🚪 مغادرة القناة")
        leave_action.triggered.connect(lambda: self._leave_telegram_channel(dialog_id, dialog_info.get('name')))

        # --- 2. خيار نسخ الرابط ---
        copy_link_action = menu.addAction("🔗 نسخ رابط القناة")
        username = dialog_info.get('username')
        if username:
            copy_link_action.triggered.connect(lambda: self._copy_channel_link(username))
        else:
            copy_link_action.setDisabled(True)
            copy_link_action.setToolTip("لا يمكن نسخ الرابط لأنها قناة خاصة أو ليس لديها اسم مستخدم.")

        menu.exec(self.tm_dialogs_tree.viewport().mapToGlobal(position))

    def _copy_channel_link(self, username):
        """ينسخ رابط القناة إلى الحافظة."""
        link = f"https://t.me/{username}"
        QApplication.clipboard().setText(link)
        self.tm_status_label.setText(f"✅ تم نسخ الرابط: {link}")

    def _leave_telegram_channel(self, dialog_id, dialog_name):
        """يبدأ عملية مغادرة القناة بعد التأكيد."""
        reply = QMessageBox.question(self, "تأكيد المغادرة",
                                     f"هل أنت متأكد أنك تريد مغادرة '{dialog_name}'؟\n"
                                     "قد لا تتمكن من الانضمام مرة أخرى إذا كانت القناة خاصة.",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No:
            return

        self.tm_status_label.setText(f"جاري مغادرة '{dialog_name}'...")
        tg_settings = self.settings.get("telegram", {})
        worker = TelegramLeaveChannelWorker(
            tg_settings["api_id"], tg_settings["api_hash"],
            tg_settings.get("session_string"), dialog_id
        )
        worker.signals.channel_left.connect(self._on_channel_left)
        self.thread_pool.start(worker)
    def _filter_tm_dialogs(self, text):
        """Filters the dialogs tree based on the search text."""
        search_text = text.lower()
        for i in range(self.tm_dialogs_tree.topLevelItemCount()):
            item = self.tm_dialogs_tree.topLevelItem(i)
            # Hide the item if the search text is not in its name
            item.setHidden(search_text not in item.text(0).lower())

    def _search_by_telegram_username(self):
        """Handles searching for a channel by username and potentially joining it."""
        username = self.tm_dialog_search.text().strip()
        if not username:
            QMessageBox.information(self, "إدخال فارغ", "يرجى كتابة اسم المستخدم أو رابط القناة في شريط البحث.")
            return

        tg_settings = self.settings.get("telegram", {})
        if not tg_settings.get("session_string"):
            QMessageBox.warning(self, "غير مسجل", "يرجى تسجيل الدخول إلى تيليجرام أولاً من صفحة الإعدادات.")
            return

        self.tm_status_label.setText(f"جاري البحث عن '{username}'...")
        self.tm_refresh_button.setEnabled(False)

        # Use a new worker for this specific action
        worker = TelegramJoinAndFetchWorker(
            tg_settings["api_id"], tg_settings["api_hash"],
            tg_settings.get("session_string"), username
        )
        worker.signals.dialogs_fetched.connect(self._on_searched_dialog_fetched) # Re-use the fetched signal
        worker.signals.dialog_fetch_error.connect(self._on_dialog_fetch_error) # Re-use the generic error handler
        self.thread_pool.start(worker)

    def _filter_tm_files(self, text):
        """Filters the files tree based on the search text."""
        search_text = text.lower()
        for i in range(self.tm_files_list.count()):
            item = self.tm_files_list.item(i)
            # Hide the item if the search text is not in its name (column 0)
            item.setHidden(search_text not in item.text().lower())

    def _fetch_telegram_dialogs(self):
        """Starts a worker to fetch the list of dialogs."""
        tg_settings = self.settings.get("telegram", {})
        if not tg_settings.get("session_string"):
            QMessageBox.warning(self, "غير مسجل", "يرجى تسجيل الدخول إلى تيليجرام أولاً من صفحة الإعدادات.")
            return

        self.tm_status_label.setText("جاري جلب قائمة القنوات والمجموعات...")
        self.tm_refresh_button.setEnabled(False)
        self.tm_dialogs_tree.clear() # مسح القائمة القديمة قبل البدء

        self.tm_rejected_dialog_count = 0 # --- جديد: إعادة تعيين عداد القنوات المرفوض
        self.tm_rejected_dialog_names = [] # --- جديد: إعادة تعيين قائمة أسماء القنوات المرفوضة
        worker = TelegramDialogsFetcher(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
        worker.signals.dialog_found.connect(self._on_dialog_found)
        worker.signals.file_fetch_finished.connect(self._on_dialog_fetch_complete)

        worker.signals.dialog_fetch_error.connect(self._on_dialog_fetch_error)
        self.thread_pool.start(worker)

    @Slot(dict)
    def _on_dialog_found(self, dialog):
        """Adds a single dialog to the tree as it's discovered."""
        # --- تحسين: فلترة المحتوى باستخدام مطابقة الكلمات الكاملة لتقليل الأخطاء ---
        dialog_name_lower = dialog.get('name', '').lower()
        # نستخدم تعبيرات النمط (regex) للبحث عن الكلمة الكاملة فقط
        # \b تعني "حدود الكلمة" (word boundary)
        # هذا يمنع مطابقة "sex" في كلمة "essex" على سبيل المثال
        if any(re.search(r'\b' + re.escape(keyword) + r'\b', dialog_name_lower) for keyword in self.FORBIDDEN_KEYWORDS) and dialog.get('id'):
            self.tm_rejected_dialog_count += 1
            self.tm_rejected_dialog_names.append({'id': dialog['id'], 'name': dialog.get('name', 'اسم غير معروف')}) # --- تعديل: تخزين ID واسم القناة المحجوبة
            return # تجاهل هذه القناة وعدم عرضها
        # --- نهاية الفلترة ---

        item = QTreeWidgetItem([dialog['name']])

        # --- تصحيح: استعادة السطر الذي تسبب في الخطأ ---
        photo_bytes = dialog.get('profile_photo_bytes')
        if photo_bytes:
            pixmap = QPixmap()
            pixmap.loadFromData(photo_bytes)
            item.setIcon(0, QIcon(pixmap))
        else:
            # أيقونة افتراضية إذا لم تكن هناك صورة
            icon_type = QStyle.StandardPixmap.SP_DriveNetIcon if dialog['is_channel'] else QStyle.StandardPixmap.SP_DirIcon
            item.setIcon(0, self.style().standardIcon(icon_type))

        item.setData(0, Qt.UserRole, dialog['id']) # Store the ID in the item
        item.setData(1, Qt.UserRole, dialog) # --- جديد: تخزين كل معلومات القناة ---
        self.tm_dialogs_tree.addTopLevelItem(item)

    @Slot(int)
    def _on_dialog_fetch_complete(self, total_count):
        """Called when the dialog fetching process is complete."""
        # --- تعديل: عرض رسالة نهائية تتضمن عدد القنوات المحجوبة ---
        displayed_count = self.tm_dialogs_tree.topLevelItemCount()
        final_message = f"اكتمل البحث. تم عرض {displayed_count} قناة/مجموعة."
        if self.tm_rejected_dialog_count > 0:
            final_message += f" (تم حجب {self.tm_rejected_dialog_count} قناة لمحتواها المخالف)."
        self.tm_status_label.setText(final_message)
        self.tm_refresh_button.setEnabled(True)

    @Slot(int, str)
    def _on_channel_left(self, dialog_id, message):
        """يتم استدعاؤها بعد نجاح مغادرة القناة."""
        self.tm_status_label.setText(message)
        # البحث عن العنصر في الشجرة وحذفه
        root = self.tm_dialogs_tree.invisibleRootItem()
        for i in range(root.childCount()):
            item = root.child(i)
            if item.data(0, Qt.UserRole) == dialog_id:
                root.removeChild(item)
                break




    @Slot(list)
    def _on_searched_dialog_fetched(self, dialogs_list):
        """Handles the result of a username search, which returns a list with one dialog."""
        self.tm_refresh_button.setEnabled(True)
        if not dialogs_list:
            self.tm_status_label.setText("لم يتم العثور على القناة.")
            return

        dialog_info = dialogs_list[0]
        # --- تعديل: إضافة القناة الجديدة إلى الشجرة وتحديدها ---
        # 1. تحقق مما إذا كانت القناة موجودة بالفعل
        root = self.tm_dialogs_tree.invisibleRootItem()
        existing_item = None
        for i in range(root.childCount()):
            item = root.child(i)
            if item.data(0, Qt.UserRole) == dialog_info['id']:
                existing_item = item
                break
        
        # 2. إذا لم تكن موجودة، أضفها
        if not existing_item:
            self._on_dialog_found(dialog_info) # إعادة استخدام دالة الإضافة
            # ابحث عنها مرة أخرى بعد الإضافة
            for i in range(root.childCount()):
                item = root.child(i)
                if item.data(0, Qt.UserRole) == dialog_info['id']:
                    existing_item = item
                    break

        # 3. حدد القناة وقم بجلب ملفاتها
        if existing_item:
            self.tm_dialogs_tree.setCurrentItem(existing_item)
            self._fetch_files_for_dialog(dialog_info['id'], dialog_info['name'])

    def _show_rejected_dialogs(self, event):
        """
        Displays a message box with the names of the rejected dialogs
        when the status label is double-clicked.
        """
        if event.button() == Qt.LeftButton and self.tm_rejected_dialog_names:
            dialog = QDialog(self)
            dialog.setWindowTitle("القنوات المحجوبة")
            dialog.setMinimumSize(400, 300)
            layout = QVBoxLayout(dialog)
            list_widget = QListWidget()
            # --- تعديل: إضافة العناصر مع تخزين بياناتها ---
            for rejected_dialog in self.tm_rejected_dialog_names:
                item = QListWidgetItem(rejected_dialog['name'])
                item.setData(Qt.UserRole, rejected_dialog) # تخزين القاموس الكامل
                list_widget.addItem(item)

            # --- جديد: تفعيل قائمة السياق ---
            list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
            list_widget.customContextMenuRequested.connect(
                lambda pos: self._show_rejected_dialog_context_menu(pos, list_widget)
            )

            layout.addWidget(list_widget)
            dialog.exec()        

    @Slot(str)
    def _on_dialog_fetch_error(self, error_message):
        self.tm_status_label.setText(f"خطأ: {error_message}")
        self.tm_refresh_button.setEnabled(True)

    def _show_rejected_dialog_context_menu(self, position, list_widget):
        """يُظهر قائمة السياق لنافذة القنوات المحجوبة."""
        item = list_widget.itemAt(position)
        if not item:
            return

        dialog_info = item.data(Qt.UserRole)
        if not dialog_info:
            return

        dialog_id = dialog_info.get('id')
        dialog_name = dialog_info.get('name')

        menu = QMenu(self)
        leave_action = menu.addAction("🚪 مغادرة القناة")
        leave_action.triggered.connect(lambda: self._leave_telegram_channel(dialog_id, dialog_name))

        # تنفيذ أمر المغادرة سيحذف القناة من تيليجرام، لكن يجب حذفها من هذه القائمة أيضاً
        leave_action.triggered.connect(lambda: self._remove_item_from_rejected_list(dialog_id, list_widget))
        menu.exec(list_widget.viewport().mapToGlobal(position))

    @Slot(QTreeWidgetItem, int)
    def _on_dialog_selected(self, item, column):
        """When a dialog is clicked, fetch its files."""
        dialog_id = item.data(0, Qt.UserRole)
        if not dialog_id: return

        self._fetch_files_for_dialog(dialog_id, item.text(0))

    def _remove_item_from_rejected_list(self, dialog_id_to_remove, list_widget):
        """يزيل عنصراً من قائمة القنوات المحجوبة بعد مغادرته."""
        # إزالته من القائمة المعروضة حالياً
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            if item and item.data(Qt.UserRole).get('id') == dialog_id_to_remove:
                list_widget.takeItem(i)
                break
        # إزالته من القائمة المصدر لكي لا يظهر في المرة القادمة
        self.tm_rejected_dialog_names = [d for d in self.tm_rejected_dialog_names if d.get('id') != dialog_id_to_remove]

    def _fetch_files_for_dialog(self, dialog_id, dialog_name):
        # --- الحل: إلغاء أي عملية جلب سابقة قبل البدء في واحدة جديدة ---
        if self.active_files_fetcher and self.active_files_fetcher.is_running():
            self.active_files_fetcher.cancel()

        self.tm_status_label.setText(f"جاري جلب الملفات من '{dialog_name}'...")
        self.tm_files_list.clear() # Clear previous file list
        self.tm_file_search.clear() # Clear search field when changing dialog

        tg_settings = self.settings.get("telegram", {})
        worker = TelegramFilesFetcher(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"), dialog_id)
        
        # --- جديد: تخزين العامل الجديد كعامل نشط ---
        self.active_files_fetcher = worker

        # --- تعديل: الربط مع الإشارات الجديدة للعرض التدريجي ---
        worker.signals.file_found.connect(self._on_file_found)
        worker.signals.file_fetch_finished.connect(self._on_file_fetch_finished)
        worker.signals.dialog_fetch_error.connect(self._on_dialog_fetch_error) # Can reuse the same error slot
        self.thread_pool.start(worker)

    @Slot(dict)
    def _on_file_found(self, file_info):
        """Adds a single file to the list as it's discovered by the worker."""
        # النص سيكون اسم الملف + الحجم في سطر جديد
        item_text = f"{file_info['title']}\n({format_size(file_info['size'])})"
        item = QListWidgetItem(item_text)
        item.setData(Qt.UserRole, file_info) # Store all info

        thumbnail_data = file_info.get('thumbnail')
        if thumbnail_data:
            pixmap = QPixmap()
            pixmap.loadFromData(thumbnail_data)
            item.setIcon(QIcon(pixmap))
        else:
            default_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
            item.setIcon(default_icon)
 
        # --- إصلاح: إضافة العناصر إلى الأسفل بدلاً من الأعلى لمنع "قفز" الواجهة ---
        # هذا يسمح للمستخدم بالتمرير بحرية أثناء جلب الملفات.
        self.tm_files_list.addItem(item)
 
        # Update status with a running count
        count = self.tm_files_list.count()
        self.tm_status_label.setText(f"جاري البحث... تم العثور على {count} ملف حتى الآن.")

    @Slot(int)
    def _on_file_fetch_finished(self, total_count):
        """Called when the file fetching process is complete."""
        self.tm_status_label.setText(f"اكتمل البحث. إجمالي الملفات: {total_count}. انقر نقراً مزدوجاً على أي ملف لبدء تحميله.")
        self.active_files_fetcher = None # Clear the active fetcher

    @Slot(QListWidgetItem)
    def _on_file_double_clicked(self, item):
        """Starts a download when a file is double-clicked."""
        file_info = item.data(Qt.UserRole)
        if not file_info: return

        # We have all the info we need, so we can create the download task directly
        # This reuses the existing Telethon download logic
        self.last_task_id += 1
        task_id = self.last_task_id
        tg_settings = self.settings.get("telegram", {})
        safe_filename = self._sanitize_filename(file_info['filename'])
        unique_filepath = find_unique_filepath(self.download_folder, safe_filename)

        worker = TelethonDownloadWorker(task_id, file_info['url'], self.download_folder, os.path.basename(unique_filepath), tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
        widget = DownloadTaskWidget(os.path.basename(unique_filepath), task_id, worker)
        self._setup_and_queue_worker(worker, widget)
        self.tabs.setCurrentWidget(self.downloader_page) # Switch to downloads tab

    def _play_selected_tm_file(self):
        """Streams the selected Telegram file directly to the player."""
        selected_items = self.tm_files_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "لم يتم التحديد", "يرجى تحديد ملف واحد لتشغيله.")
            return
        if len(selected_items) > 1:
            QMessageBox.information(self, "تحديد متعدد", "يرجى تحديد ملف واحد فقط للتشغيل المباشر.")
            return

        file_info = selected_items[0].data(Qt.UserRole)
        if not file_info: return

        # --- تعديل: التحقق من نوع الملف (فيديو، صوت، أو صورة) ---
        VIDEO_AUDIO_EXTENSIONS = ['.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv', '.mp3', '.m4a', '.ogg', '.wav']
        IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp']
        is_media = any(file_info['filename'].lower().endswith(ext) for ext in VIDEO_AUDIO_EXTENSIONS)
        is_image = any(file_info['filename'].lower().endswith(ext) for ext in IMAGE_EXTENSIONS)

        if not is_media and not is_image:
            QMessageBox.warning(self, "ملف غير مدعوم", "التشغيل المباشر مدعوم لملفات الفيديو والصوت والصور فقط.")
            return

        tg_settings = self.settings.get("telegram", {})
        if not tg_settings.get("session_string"):
            QMessageBox.warning(self, "غير مسجل", "يرجى تسجيل الدخول إلى تيليجرام أولاً.")
            return

        # --- إصلاح جذري: استخدام خادم HTTP محلي بدلاً من QIODevice ---
        # 1. إنشاء وتشغيل الخادم في خيط منفصل (للفيديو/الصوت فقط).
        streamer = LocalStreamServer(
            tg_settings["api_id"], tg_settings["api_hash"],
            tg_settings.get("session_string"), file_info['url']
        )
        streamer.start()

        # 2. الانتظار لفترة قصيرة حتى يبدأ الخادم ويحصل على منفذ (port).
        streamer.join(timeout=5.0) # انتظر حتى 5 ثوانٍ لبدء الخادم

        if streamer.error:
            QMessageBox.critical(self, "خطأ في الخادم", f"فشل تشغيل خادم البث المحلي: {streamer.error}")
            return # --- إصلاح: إيقاف الخادم عند حدوث خطأ ---

        if not streamer.local_url:
            QMessageBox.critical(self, "خطأ في الخادم", "فشل الخادم المحلي في الحصول على رابط.")
            return

        # 3. تمرير الرابط المحلي إلى المشغل.
        # --- تعديل: تمرير الرابط إلى المشغل مع إيقاف الخادم بعد إغلاق المشغل ---
        from player import MediaPlayerWindow
        # --- إصلاح: الاحتفاظ بمرجع لنافذة المشغل كمتغير تابع للكائن لمنع حذفها تلقائياً ---
        self.player_window = MediaPlayerWindow()
        self.player_window.load_video(streamer.local_url)
        # عند إغلاق نافذة المشغل، قم بإيقاف الخادم لتحرير الموارد.
        self.player_window.finished.connect(streamer.stop)
        self.player_window.show()

    def _download_selected_tm_files(self):
        """Starts downloads for all selected files in the Telegram manager files tree."""
        selected_items = self.tm_files_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self, "لم يتم التحديد", "يرجى تحديد ملف واحد على الأقل من القائمة لبدء التحميل.")
            return
        
        # Switch to the downloads tab immediately for better user experience
        self.tabs.setCurrentWidget(self.downloader_page)

        for item in selected_items:
            file_info = item.data(Qt.UserRole)
            if file_info:
                # This reuses the same logic as double-clicking a single file
                self._create_telethon_task_from_file_info(file_info)

        QMessageBox.information(self, "تمت الإضافة", f"تمت إضافة {len(selected_items)} ملفات إلى قائمة التحميلات.")

    def _create_telethon_task_from_file_info(self, file_info):
        """
        Helper function to create a Telethon download task from a file_info dictionary.
        This is used by both double-clicking and the 'download selected' button.
        """
        if not file_info: return

        self.last_task_id += 1
        task_id = self.last_task_id
        tg_settings = self.settings.get("telegram", {})
        safe_filename = self._sanitize_filename(file_info['filename'])
        unique_filepath = find_unique_filepath(self.download_folder, safe_filename)

        worker = TelethonDownloadWorker(task_id, file_info['url'], self.download_folder, os.path.basename(unique_filepath), tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
        widget = DownloadTaskWidget(os.path.basename(unique_filepath), task_id, worker)
        self._setup_and_queue_worker(worker, widget)

    def _create_tm_filter_buttons(self, parent_layout):
        """إنشاء أزرار فلترة الملفات لمدير تيليجرام."""
        self.tm_filter_buttons_layout = QHBoxLayout()
        self.tm_filter_buttons_layout.setSpacing(5)

        self.tm_filter_all_btn = QPushButton("الكل")
        self.tm_filter_media_btn = QPushButton("🖼️ الوسائط")
        self.tm_filter_files_btn = QPushButton("📄 الملفات")

        self.tm_filter_all_btn.setCheckable(True)
        self.tm_filter_media_btn.setCheckable(True)
        self.tm_filter_files_btn.setCheckable(True)

        self.tm_filter_all_btn.setChecked(True) # الزر الافتراضي

        self.tm_filter_all_btn.clicked.connect(lambda: self._apply_tm_file_filter("all"))
        self.tm_filter_media_btn.clicked.connect(lambda: self._apply_tm_file_filter("media"))
        self.tm_filter_files_btn.clicked.connect(lambda: self._apply_tm_file_filter("files"))

        self.tm_filter_buttons_layout.addWidget(self.tm_filter_all_btn)
        self.tm_filter_buttons_layout.addWidget(self.tm_filter_media_btn)
        self.tm_filter_buttons_layout.addWidget(self.tm_filter_files_btn)
        self.tm_filter_buttons_layout.addStretch()

        parent_layout.insertLayout(2, self.tm_filter_buttons_layout) # إدراج الأزرار قبل قائمة الملفات

    def _apply_tm_file_filter(self, filter_type):
        """فلترة العناصر المعروضة في قائمة ملفات تيليجرام."""
        self.tm_filter_all_btn.setChecked(filter_type == "all")
        self.tm_filter_media_btn.setChecked(filter_type == "media")
        self.tm_filter_files_btn.setChecked(filter_type == "files")

        for i in range(self.tm_files_list.count()):
            item = self.tm_files_list.item(i)
            file_info = item.data(Qt.UserRole)
            is_media = file_info.get('thumbnail') is not None # افتراض أن أي شيء له thumbnail هو وسائط
            
            if filter_type == "all": item.setHidden(False)
            elif filter_type == "media": item.setHidden(not is_media)
            elif filter_type == "files": item.setHidden(is_media)

class QRLoginDialog(QDialog):
    """A dialog to display the Telegram QR code for login."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("تسجيل الدخول إلى تيليجرام")
        self.setFixedSize(350, 450)
        self.setStyleSheet(parent.styleSheet())
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("امسح الرمز باستخدام تطبيق تيليجرام:\nالإعدادات > الأجهزة > ربط جهاز سطح المكتب", alignment=Qt.AlignCenter))
        self.qr_label = QLabel("جاري إنشاء الرمز...", alignment=Qt.AlignCenter)
        layout.addWidget(self.qr_label, 1)
    def update_qr_code(self, q_image):
        pixmap = QPixmap.fromImage(q_image)
        self.qr_label.setPixmap(pixmap.scaled(300, 300, Qt.KeepAspectRatio, Qt.SmoothTransformation))

class TelegramLoginWorker(QRunnable):
    """A dedicated worker to handle the Telethon login flow asynchronously."""
    def __init__(self, api_id, api_hash, phone):
        super().__init__()
        self.signals = WorkerSignals()
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone

        self._code = None
        self._password = None
        self._is_cancelled = False
        self._loop = None

    def set_code(self, code):
        self._code = code

    def set_password(self, password):
        self._password = password

    def cancel(self):
        self._is_cancelled = True
        if self._loop:
            self._loop.stop() # Force the event loop to stop

    @Slot()
    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            client = TelegramClient(StringSession(), self.api_id, self.api_hash, loop=loop)

            async def do_login():
                await client.connect()
                if await client.is_user_authorized():
                    return client.session.save()

                await client.send_code_request(self.phone)
                self.signals.telegram_code_required.emit()
                while self._code is None and not self._is_cancelled: await asyncio.sleep(0.1)
                if self._is_cancelled: return "تم الإلغاء من قبل المستخدم."

                try:
                    await client.sign_in(self.phone, self._code)
                except Exception: # Likely needs 2FA password
                    self.signals.telegram_password_required.emit()
                    while self._password is None and not self._is_cancelled: await asyncio.sleep(0.1)
                    if self._is_cancelled: return "تم الإلغاء من قبل المستخدم."
                    await client.sign_in(password=self._password)
                
                return client.session.save()

            session_string = loop.run_until_complete(do_login())
            self.signals.telegram_login_finished.emit(True, session_string)

        except Exception as e:
            # On failure, also emit the signal
            self.signals.telegram_login_finished.emit(False, str(e))
        finally:
            # Ensure disconnection and loop closure in all cases
            if client and client.is_connected():
                client.disconnect()
            if self._loop and self._loop.is_running():
                self._loop.close()

class TelegramQRLoginWorker(QRunnable):
    """A worker to handle the Telethon QR code login flow."""
    def __init__(self, api_id, api_hash):
        super().__init__()
        self.signals = WorkerSignals()
        self.api_id = api_id
        self.api_hash = api_hash
        self._is_cancelled = False
        self._loop = None

    def cancel(self):
        self._is_cancelled = True
        if self._loop:
            # This is a bit tricky with qr_login as it blocks.
            # We rely on the timeout of the qr_login function.
            # For a more robust solution, one might need to run the async code
            # in a way that's easier to interrupt from another thread.
            pass

    @Slot()
    def run(self):
        client = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            client = TelegramClient(StringSession(), self.api_id, self.api_hash, loop=loop)

            async def do_qr_login():
                await client.connect()
                if await client.is_user_authorized():
                    return client.session.save()

                qr_login = await client.qr_login()
                while not self._is_cancelled:
                    # Generate QR code image from the link
                    qr_img = qrcode.make(qr_login.url)
                    bio = BytesIO()
                    qr_img.save(bio, 'PNG')
                    q_image = QImage.fromData(bio.getvalue())
                    self.signals.telegram_qr_updated.emit(q_image)
                    try:
                        # Wait for the user to scan, with a timeout
                        # --- إصلاح: تحويل كائن datetime إلى timestamp قبل الطرح ---
                        # qr_login.expires هو الآن كائن datetime، لذا نستخدم .timestamp()
                        timeout_seconds = qr_login.expires.timestamp() - time.time() - 5
                        await qr_login.wait(timeout=timeout_seconds)
                        break # Succeeded
                    except asyncio.TimeoutError:
                        # QR expired, get a new one
                        await qr_login.recreate()

                if self._is_cancelled:
                    raise Exception("تم الإلغاء من قبل المستخدم.")

                # After successful scan
                return client.session.save()

            session_string = loop.run_until_complete(do_qr_login())
            self.signals.telegram_login_finished.emit(True, session_string)

        except Exception as e:
            if not self._is_cancelled:
                self.signals.telegram_login_finished.emit(False, str(e))
        finally:
            # --- إصلاح: ضمان قطع الاتصال بشكل آمن في جميع الحالات ---
            if client and client.is_connected():
                # We are in a sync context, so we can't await.
                loop.run_until_complete(client.disconnect())
            if self._loop and self._loop.is_running():
                self._loop.close()

# --- Workers for Telegram Manager ---

class TelegramDialogsFetcher(QRunnable):
    """Worker to fetch Telegram dialogs in the background."""
    def __init__(self, api_id, api_hash, session_string):
        super().__init__()
        self.signals = WorkerSignals()
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True

    @Slot()
    def run(self):
        dialog_count = 0
        try:
            manager = TelegramManager(self.api_id, self.api_hash, self.session_string)
            # --- تعديل: التعامل مع المولّد (generator) بدلاً من القائمة ---
            async_gen = manager.get_dialogs(cancellation_check=lambda: self._is_cancelled)

            async def consume_generator():
                nonlocal dialog_count
                async for dialog_data in async_gen:
                    if self._is_cancelled:
                        break
                    # إرسال كل قناة على حدة فور العثور عليها
                    self.signals.dialog_found.emit(dialog_data)
                    dialog_count += 1

            run_async_from_sync(consume_generator())

            if not self._is_cancelled:
                # إرسال إشارة اكتمال في النهاية مع العدد الإجمالي
                self.signals.file_fetch_finished.emit(dialog_count) # Re-use signal

        except Exception as e:
            if not self._is_cancelled:
                self.signals.dialog_fetch_error.emit(str(e))

class TelegramFilesFetcher(QRunnable):
    """Worker to fetch files from a specific dialog."""
    def __init__(self, api_id, api_hash, session_string, dialog_id):
        super().__init__()
        self.signals = WorkerSignals()
        # --- إصلاح: إضافة المتغيرات المفقودة التي تسببت في الخطأ ---
        self._is_cancelled = False
        self.pause_flag = threading.Event() # --- جديد: علم للتحكم في الإيقاف المؤقت
        self._is_running = False
        # --- نهاية الإصلاح ---
        self.manager = TelegramManager(api_id, api_hash, session_string)
        self.dialog_id = dialog_id

    def is_running(self):
        """Checks if the worker thread is currently active."""
        return self._is_running

    def cancel(self):
        """Signals the worker to stop its operation."""
        self._is_cancelled = True
        if hasattr(self.manager, 'cancel_current_operation'):
            self.manager.cancel_current_operation()
        self.pause_flag.set() # أيضاً قم بتفعيل علم الإيقاف لإلغاء حظر أي حلقة انتظار

    def toggle_pause_resume(self):
        """Toggles the pause state and returns the new state (True if paused)."""
        if self.pause_flag.is_set(): self.pause_flag.clear()
        else: self.pause_flag.set()
        return self.pause_flag.is_set()

    @Slot()
    def run(self):
        self._is_running = True
        file_count = 0

        def progress_reporter(messages_scanned):
            # This function is called from the async thread, so we can't update UI directly.
            # It's mainly for logging or future use.
            pass

        self._is_running = True
        try:
            # The async generator is wrapped by the sync runner
            async_gen = self.manager.get_files_from_dialog(self.dialog_id, cancellation_check=lambda: self._is_cancelled, progress_callback=progress_reporter, pause_event=self.pause_flag)

            # We must consume the generator from within an async context
            async def consume_generator():
                nonlocal file_count
                async for file_data in async_gen:
                    if self._is_cancelled: break
                    self.signals.file_found.emit(file_data)
                    file_count += 1

            run_async_from_sync(consume_generator())

            if not self._is_cancelled:
                self.signals.file_fetch_finished.emit(file_count)

        except Exception as e:
            if not self._is_cancelled:
                self.signals.dialog_fetch_error.emit(str(e))
        finally:
            self._is_running = False

class TelegramLeaveChannelWorker(QRunnable):
    """Worker to leave a Telegram channel in the background."""
    def __init__(self, api_id, api_hash, session_string, dialog_id):
        super().__init__()
        self.signals = WorkerSignals()
        self.manager = TelegramManager(api_id, api_hash, session_string)
        self.dialog_id = dialog_id

    @Slot()
    def run(self):
        try:
            message = run_async_from_sync(
                self.manager.leave_channel(self.dialog_id)
            )
            # إرسال إشارة النجاح مع معرّف القناة
            self.signals.channel_left.emit(self.dialog_id, message)
        except Exception as e:
            # في حالة الخطأ، يمكننا استخدام إشارة خطأ موجودة
            self.signals.dialog_fetch_error.emit(f"فشل المغادرة: {e}")

class TelegramJoinAndFetchWorker(QRunnable):
    """Worker to join a private channel and return its dialog info."""
    def __init__(self, api_id, api_hash, session_string, username_or_link):
        super().__init__()
        self.signals = WorkerSignals()
        self.manager = TelegramManager(api_id, api_hash, session_string)
        self.username_or_link = username_or_link

    @Slot()
    def run(self):
        try:
            # This async function will handle the logic of joining if needed
            dialog_info = run_async_from_sync(
                self.manager.join_and_get_dialog(self.username_or_link)
            )
            if dialog_info:
                # Emit as a list to match the signal's expected type
                self.signals.dialogs_fetched.emit([dialog_info])
            else:
                self.signals.dialog_fetch_error.emit("لم يتم العثور على القناة أو لا يمكن الانضمام إليها.")
        except Exception as e:
            self.signals.dialog_fetch_error.emit(str(e))

if __name__ == "__main__":
    # High-DPI scaling is enabled by default in Qt6, so AA_EnableHighDpiScaling is deprecated and no longer needed.

    app = QApplication(sys.argv)
    window = MainWindow()
    window.showMaximized() # --- تعديل: جعل البرنامج يبدأ في وضع ملء الشاشة ---
    window.show()
    sys.exit(app.exec())
