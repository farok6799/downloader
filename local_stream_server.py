import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

class LocalStreamServer(threading.Thread):
    """
    A local HTTP server that acts as a bridge between QMediaPlayer and Telethon.
    It serves the Telegram file content over a local HTTP URL.
    """
    def __init__(self, api_id, api_hash, session_string, telethon_url):
        super().__init__(daemon=True)
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.telethon_url = telethon_url
        
        self.server = None
        self.port = 0
        self.local_url = ""
        self.error = None

    def run(self):
        handler = self._create_handler()
        try:
            # Start server on an available port
            self.server = HTTPServer(('127.0.0.1', 0), handler)
            self.port = self.server.server_port
            self.local_url = f"http://127.0.0.1:{self.port}"
            self.server.serve_forever()
        except Exception as e:
            self.error = e

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()

    def _create_handler(self):
        # Create a new handler class with the required data,
        # as BaseHTTPRequestHandler doesn't easily allow passing args to its instance.
        class StreamHandler(BaseHTTPRequestHandler):
            # --- Pass required data to the class ---
            api_id = self.api_id
            api_hash = self.api_hash
            session_string = self.session_string
            telethon_url = self.telethon_url

            def do_GET(self):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.handle_request_async())
                except Exception as e:
                    self.send_error(500, f"Server Error: {e}")
                finally:
                    loop.close()

            async def handle_request_async(self):
                client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        raise Exception("Session not authorized")

                    parts = self.telethon_url.replace("telethon://", "").split('/')
                    channel_ref, msg_id = parts[0], int(parts[1])
                    entity = await client.get_entity(channel_ref)
                    message = await client.get_messages(entity, ids=msg_id)

                    if not message or not message.media:
                        self.send_error(404, "Media not found")
                        return

                    # --- إصلاح جذري: التعامل الصحيح مع أحجام الصور والملفات ---
                    # كائنات الصور (Photo) لا تحتوي على 'document' أو 'size' مباشرة.
                    # يجب الحصول على الحجم من قائمة الأحجام المتاحة.
                    if message.photo:
                        media_type = 'image/jpeg' # افتراض أن الصور هي JPEG
                        # --- إصلاح: التحقق من وجود قائمة الأحجام قبل الوصول إليها ---
                        # نأخذ أكبر حجم متاح للصورة.
                        media_size = message.photo.sizes[-1].size if hasattr(message.photo, 'sizes') and message.photo.sizes else 0
                    else: # ملف عادي (فيديو، صوت، إلخ)
                        # --- إصلاح: التحقق من وجود 'document' قبل الوصول إليه ---
                        if not hasattr(message.media, 'document'): raise Exception("Media object has no 'document' attribute.")
                        media_type = message.media.document.mime_type
                        media_size = message.media.document.size

                    self.send_response(200)
                    self.send_header('Content-type', media_type)
                    if media_size > 0:
                        self.send_header('Content-Length', str(media_size))
                    self.end_headers()

                    # Stream the file content to the response
                    async for chunk in client.iter_download(message.media):
                        try:
                            self.wfile.write(chunk)
                        except ConnectionAbortedError:
                            # The client (media player) closed the connection. This is expected.
                            # print("Client closed the stream connection.")
                            break # Stop sending data

                except Exception as e:
                    self.send_error(500, f"Streaming Error: {e}")
                finally:
                    if client.is_connected():
                        await client.disconnect()

            def log_message(self, format, *args):
                # Suppress logging to keep the console clean
                return

        return StreamHandler