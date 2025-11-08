import asyncio
import base64
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
import os
import shutil
import re
import requests 
import yt_dlp
# --- استيراد المنطق الأساسي من ملفاتك الحالية ---
# نفترض أن هذه الملفات موجودة في نفس المجلد
from downloader_core import get_download_details, get_yt_dlp_info, YTDLRunner, YTDLP_AVAILABLE, DownloadTask, find_unique_filepath, ImageConverterWorker, VideoMergerWorker, FileRepairWorker, PIL_AVAILABLE
from telegram_manager import TelegramManager, run_async_from_sync
# --- جديد: استيراد عامل تحويل Excel ---
# --- تعديل: لم نعد بحاجة إلى TelethonDirectFetcher هنا ---
from main_pyside import ExcelToPdfWorker, TelethonDirectFetcher

# --- تحميل الإعدادات (بشكل مبسط) ---
# في تطبيق حقيقي، ستحتاج إلى نظام إعدادات أكثر قوة
try:
    import json
    with open("settings.json", "r", encoding="utf-8") as f:
        SETTINGS = json.load(f)
except FileNotFoundError:
    SETTINGS = {"download_folder": "downloads"}

def save_settings():
    """يحفظ الإعدادات الحالية في ملف JSON."""
    with open("settings.json", "w", encoding="utf-8") as f:
        json.dump(SETTINGS, f, ensure_ascii=False, indent=4)


# --- تهيئة تطبيق FastAPI ---
app = FastAPI(
    title="Mostafa Downloader API",
    description="الخادم الخلفي لبرنامج التحميل، يوفر واجهات برمجية للتحميل والتحويل.",
    version="1.0.0"
)

# --- تفعيل CORS للسماح للمتصفح بالتحدث مع الخادم ---
# هذا ضروري جداً لكي تعمل الواجهة الأمامية (Frontend)
# --- تعديل جذري: السماح بجميع المصادر ("*") لحل مشكلة CORS بشكل نهائي ---
# هذا الإعداد هو الأكثر تساهلاً ويضمن أن الواجهة الأمامية على GitHub Pages
# يمكنها التواصل مع الخادم الخلفي على Railway بدون أي مشاكل.
origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # السماح بكل أنواع الطلبات (GET, POST, etc.)
    allow_headers=["*"],
)

# --- مدير اتصالات WebSocket ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, client_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

    async def send_json(self, client_id: str, data: dict):
        if client_id in self.active_connections:
            await self.active_connections[client_id].send_json(data)

manager = ConnectionManager()
# --- جديد: قاموس لتتبع مهام التحميل النشطة ---
active_workers: dict[str, YTDLRunner | DownloadTask] = {}


# --- WebSocket Endpoint لمراقبة التقدم ---
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(client_id, websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(client_id)

# --- نماذج البيانات (لتحديد شكل الطلبات) ---
class UrlRequest(BaseModel):
    url: str

class SettingsUpdate(BaseModel):
    download_folder: str | None = Field(None, description="المسار الجديد لمجلد التحميلات.")
    max_concurrent_downloads: int | None = Field(None, description="الحد الأقصى للتحميلات المتزامنة.")

class FileCompletionRequest(BaseModel):
    filepath: str
    url: str

class TelegramLoginRequest(BaseModel):
    phone: str

class TelegramCodeRequest(BaseModel):
    phone: str
    phone_code_hash: str
    code: str

class TelegramPasswordRequest(BaseModel):
    password: str

# --- 1. واجهة برمجة التطبيقات (API) لجلب تفاصيل الرابط ---
@app.post("/api/v1/details", summary="جلب تفاصيل ملف من رابط")
async def fetch_details(request: UrlRequest):
    """
    يستقبل هذا الـ endpoint رابطاً، ويحاول جلب اسم الملف وحجمه.
    يستخدم نفس الدوال الموجودة في برنامجك المكتبي.
    """
    url = request.url

    # --- تحصين: رفض الروابط غير الصالحة (مثل blob:) مبكراً ---
    if not url.lower().startswith(('http://', 'https://')):
        raise HTTPException(status_code=400, detail=f"الرابط غير صالح. يجب أن يبدأ بـ 'http://' أو 'https://'.")

    # --- تعديل جذري: التعامل مع روابط منشورات تيليجرام المباشرة بطريقة متوافقة مع الويب ---
    # التحقق إذا كان الرابط لمنشور مباشر في تيليجرام (وليس قناة عامة /s/)
    # وإذا كانت إعدادات تيليجرام موجودة.
    tg_settings = SETTINGS.get("telegram", {})
    is_telegram_post = 't.me/' in url and '/s/' not in url and tg_settings.get("session_string")
    if is_telegram_post:
        from urllib.parse import urlparse
        from telethon.sync import TelegramClient
        from telethon.sessions import StringSession # --- إصلاح: استيراد من المسار الصحيح ---
        path_parts = urlparse(url).path.strip('/').split('/')
        # التأكد من أن الرابط يحتوي على جزئين (اسم القناة/معرف الرسالة)
        if len(path_parts) == 2 and path_parts[1].isdigit():
            try:
                # --- الحل: استخدام دالة async أصلية بدلاً من العامل المعتمد على PySide ---
                details = await get_telegram_post_details_async(url, tg_settings)
                return {
                    "filename": details['filename'],
                    "total_size": details['total_size'],
                    # --- تعديل مهم: إرسال الرابط الداخلي للواجهة الأمامية ---
                    # هذا يخبر الواجهة بأنها يجب أن تستخدم عامل تحميل تيليجرام
                    "source": "telethon",
                    "internal_url": details['internal_url']
                }
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"فشل جلب معلومات منشور تيليجرام: {e}")

    # تحديد إذا كان الرابط لموقع مثل يوتيوب أم رابط مباشر
    # --- تعديل: إضافة t.me/s/ للروابط الخدمية (القنوات العامة) ---
    is_service_url = any(domain in url for domain in [
        'youtube.com', 'youtu.be', 'facebook.com', 
        'twitter.com', 'instagram.com', 'tiktok.com',
        't.me/s/' # القنوات العامة فقط
    ])

    if is_service_url:
        info, error = get_yt_dlp_info(url)
        if error:
            raise HTTPException(status_code=400, detail=f"فشل جلب المعلومات من yt-dlp: {error}")
        return {
            "filename": info.get('title', 'unknown_video'),
            "total_size": info.get('filesize') or info.get('filesize_approx'),
            "source": "yt-dlp",
            "details": info
        }
    else:
        filename, total_size, error = get_download_details(url)
        if error:
            raise HTTPException(status_code=400, detail=f"فشل جلب المعلومات من الرابط المباشر: {error}")
        return {
            "filename": filename,
            "total_size": total_size,
            "source": "direct"
        }

# --- 2. واجهة برمجة التطبيقات (API) لبدء التحميل ---
class DownloadRequest(BaseModel):
    url: str
    client_id: str
    source: str # 'direct' or 'yt-dlp'
    internal_url: str = None # --- جديد: لاستقبال الرابط الداخلي من الواجهة ---
    format_id: str = None
    audio_only: bool = False

@app.post("/api/v1/download", summary="بدء عملية تحميل جديدة")
async def start_download_legacy(request: DownloadRequest):
    """
    يبدأ عملية تحميل في الخلفية.
    يستخدم WebSockets لإرسال تحديثات التقدم.
    """
    # --- الحل: استخدام client_id من الطلب كمعرف فريد للمهمة ---
    # هذا يضمن أن الواجهة الأمامية والخلفية يتفقان على نفس المعرف.
    task_id = request.client_id

    # --- تحسين: استخدام دالة callback موحدة لإرسال التحديثات ---
    loop = asyncio.get_running_loop()
    def progress_callback(data):
        # --- الحل: إرسال التحديثات إلى client_id الخاص باتصال WebSocket ---
        # المُعرّف الذي تم إنشاؤه في الواجهة (`task_id`) يُستخدم فقط لتعريف عنصر التحميل.
        # أما المُعرّف الذي يُستخدم لإرسال الرسائل عبر WebSocket فهو مُعرّف الاتصال نفسه.
        # بما أننا لا نمرر معرف الاتصال الأصلي، سنفترض أن task_id يحتوي على الجزء الأول منه.
        websocket_client_id = task_id.split('-')[0] + '-' + task_id.split('-')[1] + '-' + task_id.split('-')[2]
        asyncio.run_coroutine_threadsafe(manager.send_json(websocket_client_id, data), loop)

    worker = None
    if request.source == 'yt-dlp':
        if not YTDLP_AVAILABLE:
            raise HTTPException(status_code=500, detail="مكتبة yt-dlp غير مثبتة على الخادم.")
        
        # تعديل YTDLRunner ليقبل دالة callback
        # هذا يتطلب تعديل YTDLRunner في downloader_core.py
        worker = YTDLRunner(
            task_id=task_id,
            url=request.url,
            download_folder=SETTINGS['download_folder'],
            update_callback=progress_callback,
            format_id=request.format_id,
            audio_only=request.audio_only
        )
    # --- جديد: التعامل مع طلبات تحميل تيليجرام ---
    elif request.source == 'telethon' and request.internal_url:
        # جلب التفاصيل مرة أخرى للتأكد (اختياري لكنه جيد للتحقق)
        # في هذه الحالة، التفاصيل موجودة بالفعل من الخطوة السابقة
        # لذا سنقوم بإنشاء العامل مباشرة
        from main_pyside import TelethonDownloadWorker
        # نحتاج إلى اسم الملف من الرابط الداخلي، يمكن تحسين هذا لاحقاً
        filename = f"telegram_file_{task_id}" # اسم مؤقت
        filepath = find_unique_filepath(SETTINGS['download_folder'], filename)

        worker = TelethonDownloadWorker(
            task_id=task_id,
            telethon_url=request.internal_url,
            download_folder=SETTINGS['download_folder'],
            filename=os.path.basename(filepath),
            api_id=SETTINGS['telegram']['api_id'],
            api_hash=SETTINGS['telegram']['api_hash'],
            session_string=SETTINGS['telegram']['session_string'],
            update_callback=progress_callback,
            environment='web'
        )
    elif request.source == 'direct':
        # جلب التفاصيل مرة أخرى للتأكد من صحتها قبل البدء
        filename, total_size, error = get_download_details(request.url)
        if error:
            raise HTTPException(status_code=400, detail=f"فشل التحقق من الرابط المباشر: {error}")

        filepath = find_unique_filepath(SETTINGS['download_folder'], filename)

        worker = DownloadTask(
            task_id=task_id,
            url=request.url,
            filepath=filepath,
            total_size=total_size,
            # --- الحل: تمرير دالة الـ callback مباشرة ---
            # هذا يوحد طريقة إرسال التحديثات لجميع أنواع التحميل.
            update_callback=progress_callback,
        )
    else:
        raise HTTPException(status_code=400, detail="مصدر التحميل غير معروف.")

    # --- الحل: تشغيل الخيط مباشرة بدلاً من استخدام BackgroundTasks ---
    # هذا يضمن أن الخيط يعمل في سياق يمكنه الوصول إلى حلقة الأحداث
    # بشكل صحيح لإرسال تحديثات WebSocket.
    active_workers[task_id] = worker
    worker.start() # استخدام .start() بدلاً من .run() لتشغيله في خيط جديد

    # لا نحتاج إلى cleanup_task هنا لأن العامل سيتم تنظيفه عند الإلغاء أو الانتهاء
    return {"status": "success", "message": f"بدأ تحميل الرابط: {request.url}"}

async def get_telegram_post_details_async(url: str, tg_settings: dict) -> dict:
    """
    دالة غير متزامنة أصلية (Native Async) لجلب تفاصيل منشور تيليجرام.
    هذه الدالة متوافقة تماماً مع FastAPI وتتجنب مشاكل التوافق مع PySide.
    """
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession # --- إصلاح: استيراد من المسار الصحيح ---
    from urllib.parse import urlparse

    client = None
    try:
        # استخدام with لضمان قطع الاتصال تلقائياً
        async with TelegramClient(StringSession(tg_settings.get("session_string")), 
                                  tg_settings["api_id"], 
                                  tg_settings["api_hash"]) as client:

            if not await client.is_user_authorized():
                raise Exception("جلسة Telethon غير صالحة. يرجى تسجيل الدخول من الإعدادات.")

            # استخراج اسم القناة ومعرف الرسالة من الرابط
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            channel_ref, msg_id = path_parts[0], int(path_parts[1])

            channel_entity = await client.get_entity(channel_ref)
            message = await client.get_messages(channel_entity, ids=msg_id)

            if not message or not (message.file or message.photo):
                raise Exception("لم يتم العثور على ملف أو صورة في هذا المنشور.")

            # التعامل مع حجم واسم الملفات والصور
            if message.photo:
                filename = f"telegram_{channel_ref}_{msg_id}.jpg"
                total_size = getattr(message.photo.sizes[-1], 'size', None) if message.photo.sizes else None
            else: # ملف عادي
                filename = getattr(message.file, 'name', f"telegram_file_{msg_id}")
                total_size = getattr(message.file, 'size', None)

            # إنشاء الرابط الداخلي الذي يفهمه عامل التحميل
            internal_url = f"telethon://{getattr(channel_entity, 'username', channel_ref)}/{msg_id}"

            return {
                "internal_url": internal_url,
                "filename": filename,
                "total_size": total_size
            }
    except Exception as e:
        # إعادة إرسال الخطأ ليتم عرضه في الواجهة
        raise e


# --- جديد: نقطة نهاية لبث التحميل مباشرة إلى المتصفح ---
@app.get("/api/v1/stream", summary="بث ملف للتحميل المباشر في المتصفح")
async def stream_download(url: str, filename: str, source: str, format_id: str = None):
    """
    يبث محتوى الملف مباشرة إلى المتصفح لتفعيل التحميل الأصلي.
    """
    # --- جديد: التعامل مع بث ملفات تيليجرام ---
    if source == 'telethon':
        tg_settings = SETTINGS.get("telegram", {})
        if not tg_settings.get("session_string"):
            raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام.")

        from telethon.sync import TelegramClient
        from telethon.sessions import StringSession

        async def telegram_stream_generator():
            """
            مولّد (Generator) غير متزامن يقوم بجلب أجزاء الملف من تيليجرام وبثها.
            """
            client = TelegramClient(StringSession(tg_settings["session_string"]), tg_settings["api_id"], tg_settings["api_hash"])
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    raise Exception("جلسة Telethon غير صالحة.")

                # استخراج معرف الرسالة من الرابط الداخلي (telethon://...)
                parts = url.replace("telethon://", "").split('/')
                channel_ref, msg_id = parts[0], int(parts[1])
                entity = await client.get_entity(channel_ref)
                message = await client.get_messages(entity, ids=msg_id)

                if not message or not message.media:
                    raise Exception("لم يتم العثور على وسائط في الرسالة.")

                # بث محتوى الملف على شكل أجزاء
                async for chunk in client.iter_download(message.media):
                    yield chunk

            except Exception as e:
                # في حالة حدوث خطأ، يمكننا تسجيله ولكن لا يمكننا إرسال استجابة HTTP أخرى
                print(f"خطأ أثناء بث تيليجرام: {e}")
            finally:
                if client.is_connected():
                    await client.disconnect()

        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Type': 'application/octet-stream',
        }
        # إرجاع StreamingResponse الذي يستخدم المولّد لبث البيانات
        return StreamingResponse(telegram_stream_generator(), headers=headers)

    if source == 'direct':
        try:
            # استخدام requests لبدء جلب الملف كـ stream
            response = requests.get(url, stream=True, allow_redirects=True, timeout=30)
            response.raise_for_status()

            # إعداد الترويسات اللازمة لتفعيل التحميل في المتصفح
            headers = {
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Content-Type': 'application/octet-stream',
            }
            # تمرير حجم الملف إذا كان معروفاً
            if 'Content-Length' in response.headers:
                headers['Content-Length'] = response.headers['Content-Length']

            # إرجاع StreamingResponse الذي يقوم ببث المحتوى
            return StreamingResponse(response.iter_content(chunk_size=8192), headers=headers)

        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=500, detail=f"فشل الاتصال بالرابط المصدر: {e}")

    elif source == 'yt-dlp':
        if not YTDLP_AVAILABLE:
            raise HTTPException(status_code=501, detail="مكتبة yt-dlp غير مثبتة على الخادم.")

        # --- الحل: استخدام yt-dlp لجلب رابط التحميل المباشر فقط ---
        # هذا أسرع بكثير من تحميل الملف على الخادم أولاً
        ydl_opts = {
            'format': format_id if format_id else 'best',
            'quiet': True,
            'noplaylist': True,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                # الحصول على رابط التحميل المباشر الذي وجده yt-dlp
                direct_url = info.get('url')
                if not direct_url:
                    raise HTTPException(status_code=500, detail="فشل yt-dlp في استخراج رابط التحميل المباشر.")
                
                # إعادة توجيه المتصفح مباشرة إلى هذا الرابط
                return FileResponse(path=direct_url, filename=filename, media_type='application/octet-stream')

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"خطأ من yt-dlp: {e}")

    raise HTTPException(status_code=400, detail="مصدر التحميل غير مدعوم للبث المباشر.")

# --- 3. واجهة برمجة التطبيقات (API) لإلغاء التحميل ---
@app.post("/api/v1/cancel/{client_id}", summary="إلغاء تحميل نشط")
async def cancel_download(client_id: str):
    """
    يبحث عن العامل النشط بالـ ID المحدد ويستدعي دالة الإلغاء الخاصة به.
    """
    worker = active_workers.get(client_id)
    if not worker:
        raise HTTPException(status_code=404, detail="لم يتم العثور على مهمة التحميل.")

    worker.cancel() # استدعاء دالة الإلغاء في العامل
    # سيتم تنظيف العامل من القاموس تلقائياً عند انتهاء الخيط

    return {"status": "success", "message": f"تم إرسال طلب الإلغاء للمهمة: {client_id}"}

# --- جديد: واجهة برمجة التطبيقات (API) للإيقاف المؤقت والاستئناف ---
@app.post("/api/v1/pause/{client_id}", summary="إيقاف تحميل نشط مؤقتاً")
async def pause_download(client_id: str):
    """
    يستدعي دالة toggle_pause_resume في العامل لإيقاف التحميل مؤقتاً.
    """
    worker = active_workers.get(client_id)
    if not worker:
        raise HTTPException(status_code=404, detail="لم يتم العثور على مهمة التحميل.")
    
    if hasattr(worker, 'toggle_pause_resume'):
        worker.toggle_pause_resume()
        return {"status": "success", "message": f"تم إرسال طلب الإيقاف المؤقت للمهمة: {client_id}"}
    else:
        raise HTTPException(status_code=400, detail="هذا النوع من التحميل لا يدعم الإيقاف المؤقت.")

@app.post("/api/v1/resume/{client_id}", summary="استئناف تحميل متوقف")
async def resume_download(client_id: str):
    """
    يستدعي دالة toggle_pause_resume في العامل لاستئناف التحميل.
    """
    worker = active_workers.get(client_id)
    if not worker:
        raise HTTPException(status_code=404, detail="لم يتم العثور على مهمة التحميل.")

    if hasattr(worker, 'toggle_pause_resume'):
        worker.toggle_pause_resume()
        return {"status": "success", "message": f"تم إرسال طلب الاستئناف للمهمة: {client_id}"}
    else:
        raise HTTPException(status_code=400, detail="هذا النوع من التحميل لا يدعم الاستئناف.")

# --- جديد: واجهة برمجة التطبيقات (API) لبدء تحميل من تيليجرام ---
class TelegramDownloadRequest(BaseModel):
    client_id: str      # معرّف المهمة الفريد من الواجهة
    file_info: dict     # قاموس يحتوي على كل معلومات الملف من تيليجرام

@app.post("/api/v1/telegram/download", summary="بدء تحميل ملف من تيليجرام")
async def start_telegram_download(request: TelegramDownloadRequest):
    """
    يبدأ تحميل ملف محدد من تيليجرام باستخدام `TelethonDownloadWorker`.
    """
    tg_settings = SETTINGS.get("telegram", {})
    if not tg_settings.get("session_string"):
        raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام.")

    task_id = request.client_id
    file_info = request.file_info
    
    # --- استخدام نفس دالة الـ callback لإرسال التحديثات ---
    loop = asyncio.get_running_loop()
    def progress_callback(data):
        # معرف الاتصال الفعلي موجود في أول جزئين من معرف المهمة
        websocket_client_id = '-'.join(task_id.split('-', 3)[:3])
        asyncio.run_coroutine_threadsafe(manager.send_json(websocket_client_id, data), loop)

    # التأكد من أن اسم الملف آمن للاستخدام في نظام الملفات
    safe_filename = re.sub(r'[\\/*?:"<>|]', "", file_info.get('filename', f"telegram_file_{task_id}"))
    filepath = find_unique_filepath(SETTINGS['download_folder'], safe_filename)

    # --- جديد: استيراد واستخدام TelethonDownloadWorker ---
    from main_pyside import TelethonDownloadWorker
    worker = TelethonDownloadWorker(
        task_id=task_id,
        telethon_url=file_info['url'],
        download_folder=SETTINGS['download_folder'],
        filename=os.path.basename(filepath),
        api_id=tg_settings["api_id"],
        api_hash=tg_settings["api_hash"],
        session_string=tg_settings.get("session_string"),
        update_callback=progress_callback, # تمرير الـ callback مباشرة
        environment='web'  # تحديد البيئة كـ "web"
    )

    active_workers[task_id] = worker
    worker.start()
    return {"status": "success", "message": f"بدأ تحميل ملف تيليجرام: {safe_filename}"}

# --- 3. واجهة برمجة التطبيقات (API) لجلب قنوات تيليجرام ---
@app.get("/api/v1/telegram/dialogs", summary="جلب قائمة قنوات ومجموعات تيليجرام")
async def get_telegram_dialogs():
    """
    يستخدم TelegramManager لجلب قائمة القنوات والمجموعات الخاصة بالمستخدم.
    """
    tg_settings = SETTINGS.get("telegram", {})
    if not tg_settings.get("session_string"):
        raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام. يرجى تسجيل الدخول من الإعدادات أولاً.")

    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
    
    try:
        # FastAPI يتعامل مع الدوال غير المتزامنة (async) بشكل ممتاز
        processed_dialogs = []
        async for dialog in manager.get_dialogs():
            # --- الحل: تحويل بايتات الصورة إلى Base64 قبل إرسالها ---
            if dialog.get("profile_photo_bytes"):
                # ترميز البايتات إلى سلسلة نصية Base64
                base64_image = base64.b64encode(dialog["profile_photo_bytes"]).decode('utf-8')
                dialog["profile_photo_bytes"] = base64_image
            processed_dialogs.append(dialog)
            
        return {"dialogs": processed_dialogs}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"حدث خطأ أثناء الاتصال بتيليجرام: {e}")

# --- جديد: واجهة برمجة التطبيقات (API) لجلب الملفات من قناة محددة ---
@app.get("/api/v1/telegram/dialog/{dialog_id}/files", summary="جلب قائمة الملفات من قناة أو مجموعة")
async def get_telegram_files(dialog_id: int):
    """
    يستخدم TelegramManager لجلب قائمة الملفات من حوار (dialog) محدد.
    يستخدم StreamingResponse لإرسال الملفات تدريجياً فور العثور عليها.
    """
    tg_settings = SETTINGS.get("telegram", {})
    if not tg_settings.get("session_string"):
        raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام.")

    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))

    async def file_generator():
        """مولّد غير متزامن يرسل كل ملف كسطر JSON."""
        try:
            async for file_data in manager.get_files_from_dialog(dialog_id):
                # تحويل البيانات إلى JSON وإرسالها مع سطر جديد
                # --- الحل: التحقق من وجود الصورة المصغرة وتحويلها إلى Base64 ---
                # هذا يمنع خطأ "not JSON serializable" عند التعامل مع الصور.
                if file_data.get("thumbnail"):
                    base64_thumb = base64.b64encode(file_data["thumbnail"]).decode('utf-8')
                    file_data["thumbnail"] = base64_thumb

                yield json.dumps(file_data, ensure_ascii=False) + "\n"
        except Exception as e:
            # لا يمكننا إرسال HTTPException من داخل المولّد، لذا نرسل رسالة خطأ كجزء من البث
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(file_generator(), media_type="application/x-ndjson")

# --- 4. واجهة برمجة التطبيقات (API) للأدوات ---
@app.post("/api/v1/tools/excel-to-pdf", summary="تحويل ملف Excel إلى PDF")
async def convert_excel_to_pdf(file: UploadFile = File(...)):
    """
    يستقبل ملف Excel، يحوله إلى PDF، ويعيد الملف الناتج.
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="الملف المرفوع ليس ملف Excel صالح.")

    # إنشاء مجلد مؤقت لتخزين الملفات
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)

    input_path = os.path.join(temp_dir, file.filename)
    output_filename = f"{os.path.splitext(file.filename)[0]}.pdf"
    output_path = os.path.join(temp_dir, output_filename)

    # حفظ الملف المرفوع مؤقتاً
    with open(input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        # استخدام نفس العامل من برنامج سطح المكتب (بعد تعديل بسيط)
        # ملاحظة: هذا العامل يعمل في الخيط الرئيسي، للتحويلات الكبيرة يجب وضعه في BackgroundTasks
        worker = ExcelToPdfWorker(input_path, output_path)
        worker.run() # تشغيل مباشر

        # إتاحة الملف المحول للتحميل
        return FileResponse(path=output_path, media_type='application/pdf', filename=output_filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل التحويل: {e}")

# --- جديد: واجهات برمجية للأدوات الإضافية ---

@app.post("/api/v1/tools/image-converter", summary="تحويل صيغة صورة")
async def convert_image(
    file: UploadFile = File(...),
    output_format: str = Form(...)
):
    if not PIL_AVAILABLE:
        raise HTTPException(status_code=501, detail="مكتبة Pillow غير مثبتة على الخادم.")

    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    input_path = os.path.join(temp_dir, file.filename)
    
    base_name = os.path.splitext(file.filename)[0]
    output_filename = f"{base_name}.{output_format.lower()}"
    output_path = os.path.join(temp_dir, output_filename)

    with open(input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # لا يمكننا استخدام worker هنا لأنه سيعيد الاستجابة قبل انتهاء التحويل
    # سنجري التحويل مباشرة وننتظره
    try:
        converter = ImageConverterWorker(input_path, output_path, output_format)
        converter.run() # تشغيل مباشر في نفس الخيط
        return FileResponse(path=output_path, media_type=f'image/{output_format.lower()}', filename=output_filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل تحويل الصورة: {e}")

@app.post("/api/v1/tools/video-merger", summary="دمج فيديو وصوت")
async def merge_video_audio(
    video_file: UploadFile = File(...),
    audio_file: UploadFile = File(...)
):
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)

    video_path = os.path.join(temp_dir, video_file.filename)
    audio_path = os.path.join(temp_dir, audio_file.filename)

    with open(video_path, "wb") as buffer:
        shutil.copyfileobj(video_file.file, buffer)
    with open(audio_path, "wb") as buffer:
        shutil.copyfileobj(audio_file.file, buffer)

    base_name, ext = os.path.splitext(video_file.filename)
    output_filename = f"{base_name}_merged.mp4"
    output_path = os.path.join(temp_dir, output_filename)

    try:
        merger = VideoMergerWorker(video_path, audio_path, output_path)
        merger.run()
        return FileResponse(path=output_path, media_type='video/mp4', filename=output_filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل الدمج: {e}")

@app.post("/api/v1/tools/file-repair", summary="إصلاح ملف فيديو")
async def repair_file(
    file: UploadFile = File(...),
    deep_repair: bool = Form(False)
):
    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    input_path = os.path.join(temp_dir, file.filename)

    with open(input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    base_name, ext = os.path.splitext(file.filename)
    output_filename = f"{base_name}_repaired{ext}"
    output_path = os.path.join(temp_dir, output_filename)

    try:
        repairer = FileRepairWorker(input_path, output_path, deep_repair)
        repairer.run()
        return FileResponse(path=output_path, media_type=file.content_type, filename=output_filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل الإصلاح: {e}")

# --- 5. واجهات برمجة التطبيقات (API) للإعدادات والسجل ---

@app.get("/api/v1/settings", summary="جلب الإعدادات الحالية")
async def get_settings():
    """يعيد قاموساً يحتوي على الإعدادات الحالية للبرنامج."""
    return SETTINGS

@app.post("/api/v1/settings", summary="تحديث الإعدادات")
async def update_settings(new_settings: SettingsUpdate):
    """يستقبل إعدادات جديدة ويقوم بتحديثها وحفظها."""
    updated = False
    if new_settings.download_folder is not None:
        SETTINGS["download_folder"] = new_settings.download_folder
        os.makedirs(new_settings.download_folder, exist_ok=True)
        updated = True
    if new_settings.max_concurrent_downloads is not None:
        SETTINGS["max_concurrent_downloads"] = new_settings.max_concurrent_downloads
        updated = True
    
    if updated:
        save_settings()
        return {"status": "success", "message": "تم تحديث الإعدادات بنجاح."}
    else:
        return {"status": "no_change", "message": "لم يتم تقديم أي تغييرات."}

@app.get("/api/v1/log", summary="جلب سجل التحميلات المكتملة")
async def get_download_log():
    """يعيد قائمة بالتحميلات التي تمت (من ملف الإعدادات)."""
    return {"log": SETTINGS.get("completed_log", [])}

@app.delete("/api/v1/log", summary="مسح سجل التحميلات")
async def clear_download_log():
    """يمسح سجل التحميلات من ملف الإعدادات."""
    if "completed_log" in SETTINGS:
        SETTINGS["completed_log"] = []
        save_settings()
    return {"status": "success", "message": "تم مسح سجل التحميلات."}

@app.get("/api/v1/library", summary="تصفح مكتبة الملفات المحملة")
async def browse_library():
    """
    يقوم بفحص مجلد التحميلات ويعيد قائمة بالملفات الموجودة مع أحجامها.
    """
    download_folder = SETTINGS.get("download_folder", "downloads")
    if not os.path.isdir(download_folder):
        raise HTTPException(status_code=404, detail="مجلد التحميلات غير موجود.")

    files_list = []
    for root, _, files in os.walk(download_folder):
        for filename in files:
            full_path = os.path.join(root, filename)
            try:
                file_size = os.path.getsize(full_path)
                relative_folder = os.path.relpath(root, download_folder)
                if relative_folder == ".":
                    relative_folder = "الرئيسي"
                
                files_list.append({
                    "filename": filename,
                    "folder": relative_folder,
                    "size": file_size,
                    "path": full_path # المسار الكامل للاستخدامات المستقبلية
                })
            except OSError:
                continue
    return {"files": files_list}

# --- 6. واجهات برمجة إضافية لمدير تيليجرام والأدوات ---

@app.post("/api/v1/telegram/search", summary="البحث عن قناة والانضمام إليها")
async def search_and_join_telegram_channel(request: UrlRequest):
    """
    يبحث عن قناة عامة أو خاصة باستخدام اسمها أو رابطها، وينضم إليها إذا لزم الأمر.
    """
    tg_settings = SETTINGS.get("telegram", {})
    if not tg_settings.get("session_string"):
        raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام.")

    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
    try:
        dialog_info = await run_async_from_sync(manager.join_and_get_dialog(request.url))
        if dialog_info:
            return dialog_info
        else:
            raise HTTPException(status_code=404, detail="لم يتم العثور على القناة أو لا يمكن الانضمام إليها.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/telegram/leave/{dialog_id}", summary="مغادرة قناة أو مجموعة")
async def leave_telegram_channel(dialog_id: int):
    """يغادر القناة أو المجموعة المحددة بالمعرف الخاص بها."""
    tg_settings = SETTINGS.get("telegram", {})
    if not tg_settings.get("session_string"):
        raise HTTPException(status_code=401, detail="لم يتم تسجيل الدخول إلى تيليجرام.")

    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"], tg_settings.get("session_string"))
    try:
        message = await run_async_from_sync(manager.leave_channel(dialog_id))
        return {"status": "success", "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/tools/file-completion", summary="استكمال تحميل ملف من رابط")
async def complete_file_download(request: FileCompletionRequest):
    """
    يستقبل مسار ملف محلي ورابطه الأصلي، ويحاول تحميل الجزء المتبقي.
    هذه العملية تعمل في الخيط الرئيسي، لذا قد تكون بطيئة للواجهة.
    """
    if not os.path.exists(request.filepath):
        raise HTTPException(status_code=404, detail="الملف المحلي غير موجود.")

    # لا يمكننا استخدام العامل هنا لأنه سيعيد الاستجابة قبل الانتهاء.
    # سنقوم بالعملية مباشرة.
    try:
        local_size = os.path.getsize(request.filepath)
        
        with requests.head(request.url, allow_redirects=True, timeout=15, verify=False) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))

        if local_size >= total_size:
            return {"status": "complete", "message": "الملف مكتمل بالفعل."}

        range_header = {'Range': f'bytes={local_size}-'}
        with requests.get(request.url, stream=True, headers=range_header, timeout=60, verify=False) as r:
            r.raise_for_status()
            with open(request.filepath, 'ab') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        return {"status": "success", "message": f"تم استكمال الملف بنجاح. الحجم الجديد: {total_size}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل استكمال الملف: {e}")

@app.post("/api/v1/tools/combine-parts", summary="دمج أجزاء تحميل يدوياً")
async def combine_parts(file: UploadFile = File(...)):
    """
    يستقبل أول ملف جزء (.part0) ويقوم بدمج كل الأجزاء التالية له.
    """
    if not file.filename.endswith(".part0"):
        raise HTTPException(status_code=400, detail="يرجى اختيار أول ملف في السلسلة (ينتهي بـ .part0).")

    temp_dir = "temp_uploads"
    os.makedirs(temp_dir, exist_ok=True)
    first_part_path = os.path.join(temp_dir, file.filename)
    with open(first_part_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    base_path = first_part_path.rsplit('.part', 1)[0]
    output_path = base_path
    
    try:
        with open(output_path, 'wb') as final_file:
            i = 0
            while True:
                part_file = f"{base_path}.part{i}"
                if not os.path.exists(part_file): break
                with open(part_file, 'rb') as pf: shutil.copyfileobj(pf, final_file)
                i += 1
        return FileResponse(path=output_path, filename=os.path.basename(output_path))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"فشل الدمج اليدوي: {e}")

# --- 7. واجهات برمجة التطبيقات (API) لتسجيل الدخول إلى تيليجرام ---

@app.post("/api/v1/telegram/login/send-code", summary="إرسال كود التحقق إلى رقم هاتف")
async def telegram_send_code(request: TelegramLoginRequest):
    """
    يبدأ عملية تسجيل الدخول بإرسال كود تحقق إلى رقم الهاتف المحدد.
    """
    tg_settings = SETTINGS.get("telegram", {})
    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"])
    try:
        # run_async_from_sync لا يعمل بشكل جيد مع دوال تسجيل الدخول
        # لذا سنستخدم المنطق غير المتزامن مباشرة
        phone_code_hash = await manager.send_code_request(request.phone)
        return {"status": "success", "phone_code_hash": phone_code_hash}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/telegram/login/submit-code", summary="إرسال كود التحقق وكلمة المرور")
async def telegram_submit_code(request: TelegramCodeRequest):
    """
    يتحقق من الكود المرسل. إذا كان الحساب يتطلب كلمة مرور، سيعيد خطأً بذلك.
    إذا نجح، سيحفظ جلسة المستخدم.
    """
    tg_settings = SETTINGS.get("telegram", {})
    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"])
    try:
        session_string = await manager.sign_in(
            phone=request.phone,
            phone_code_hash=request.phone_code_hash,
            code=request.code
        )
        # إذا نجح تسجيل الدخول، قم بتحديث وحفظ الإعدادات
        SETTINGS["telegram"]["session_string"] = session_string
        SETTINGS["telegram"]["phone"] = request.phone
        save_settings()
        return {"status": "success", "message": "تم تسجيل الدخول بنجاح!"}
    except Exception as e:
        # التحقق مما إذا كان الخطأ بسبب الحاجة إلى كلمة مرور
        if "password" in str(e).lower():
            raise HTTPException(status_code=401, detail="password_required")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/telegram/login/submit-password", summary="إرسال كلمة مرور التحقق بخطوتين")
async def telegram_submit_password(request: TelegramPasswordRequest):
    """
    يتحقق من كلمة مرور التحقق بخطوتين ويُكمل عملية تسجيل الدخول.
    """
    tg_settings = SETTINGS.get("telegram", {})
    # يفترض أن يكون المدير قد تم تهيئته من الخطوة السابقة
    # هذا النهج مبسط، في تطبيق حقيقي قد تحتاج إلى إدارة حالة العميل بشكل أفضل
    manager = TelegramManager(tg_settings["api_id"], tg_settings["api_hash"])
    
    # إعادة الاتصال بنفس العميل الذي طلب الكود
    # هذا الجزء معقد بدون إدارة جلسات المستخدمين، سنعتمد على أن العميل لا يزال موجوداً
    if not manager.is_connected_for_login():
         raise HTTPException(status_code=400, detail="انتهت صلاحية جلسة تسجيل الدخول. يرجى البدء من جديد.")

    try:
        session_string = await manager.check_password(request.password)
        SETTINGS["telegram"]["session_string"] = session_string
        save_settings()
        return {"status": "success", "message": "تم تسجيل الدخول بنجاح!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- جديد: واجهة برمجية للتحقق من وجود ffmpeg ---
@app.get("/api/v1/tools/check-ffmpeg", summary="التحقق من تثبيت ffmpeg")
async def check_ffmpeg():
    is_installed = shutil.which('ffmpeg') is not None
    return {"installed": is_installed}

# --- جديد: واجهة برمجية لتثبيت ffmpeg ---
@app.post("/api/v1/tools/install-ffmpeg", summary="تثبيت ffmpeg تلقائياً")
async def install_ffmpeg(background_tasks: BackgroundTasks):
    # سنستخدم عاملاً من main_pyside لأنه يحتوي على منطق التثبيت
    from main_pyside import FfmpegInstallerWorker
    
    # لا يمكننا إرجاع حالة التقدم هنا بسهولة عبر HTTP
    # لذا سنقوم بتشغيله في الخلفية ونأمل أن ينجح
    worker = FfmpegInstallerWorker()
    background_tasks.add_task(worker.run)
    
    return {"status": "started", "message": "بدأت عملية تثبيت ffmpeg في الخلفية. قد تستغرق بعض الوقت."}

# --- جديد: واجهة برمجية لتحديث yt-dlp ---
@app.post("/api/v1/tools/update-ytdlp", summary="تحديث مكتبة yt-dlp")
async def update_ytdlp(background_tasks: BackgroundTasks):
    from main_pyside import YtdlpUpdaterWorker
    
    # هذا أيضاً سيعمل في الخلفية
    worker = YtdlpUpdaterWorker()
    background_tasks.add_task(worker.run)
    
    return {"status": "started", "message": "بدأت عملية تحديث yt-dlp في الخلفية."}


# --- نقطة بداية تشغيل الخادم ---
if __name__ == "__main__":
    # --- تعديل: الكود الجديد للتوافق مع Railway ---
    # هذا الكود يقرأ المنفذ (PORT) من متغيرات البيئة التي توفرها Railway
    # ويجعل الخادم يستمع على كل الواجهات (0.0.0.0) ليعمل بشكل صحيح.
    port = int(os.environ.get("PORT", 8000)) # استخدام 8000 كقيمة افتراضية للتشغيل المحلي
    uvicorn.run("web_main:app", host="0.0.0.0", port=port)