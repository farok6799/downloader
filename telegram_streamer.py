import asyncio
import threading
from PySide6.QtCore import QIODevice, Signal, QObject
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

class StreamSignals(QObject):
    """Defines signals for the streaming device."""
    error = Signal(str)
    ready_to_play = Signal()
    finished = Signal()

class TelegramStreamDevice(QIODevice):
    """
    A custom QIODevice that streams a Telegram file chunk by chunk.
    This allows QMediaPlayer to play a file without downloading it first.
    """
    def __init__(self, api_id, api_hash, session_string, telethon_url, parent=None):
        super().__init__(parent)
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.telethon_url = telethon_url

        self.client = None
        self.message = None
        self.file_size = 0
        self.buffer = bytearray()
        self.position = 0
        self.is_open = False

        self.signals = StreamSignals()
        self.background_thread = threading.Thread(target=self._run_async_setup, daemon=True)

    def start_stream(self):
        """Starts the background thread to connect and fetch metadata."""
        self.background_thread.start()

    def _run_async_setup(self):
        """Runs the asyncio event loop in a separate thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._async_setup())
        except Exception as e:
            self.signals.error.emit(f"خطأ في البث: {e}")
        finally:
            if self.client and self.client.is_connected():
                loop.run_until_complete(self.client.disconnect())
            loop.close()

    async def _async_setup(self):
        """Connects to Telegram and gets the file metadata."""
        try:
            self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
            await self.client.connect()
            if not await self.client.is_user_authorized():
                raise Exception("جلسة Telethon غير صالحة.")

            parts = self.telethon_url.replace("telethon://", "").split('/')
            channel_ref, msg_id = parts[0], int(parts[1])
            entity = await self.client.get_entity(channel_ref)
            self.message = await self.client.get_messages(entity, ids=msg_id)

            if not self.message or not self.message.media:
                raise Exception("لم يتم العثور على وسائط في الرسالة.")

            self.file_size = self.message.file.size
            self.open(QIODevice.ReadOnly)
            self.is_open = True
            self.signals.ready_to_play.emit()

        except Exception as e:
            self.signals.error.emit(f"فشل تهيئة البث: {e}")

    def size(self):
        return self.file_size

    def atEnd(self):
        return self.position >= self.file_size

    def readData(self, max_size):
        """Called by QMediaPlayer to get the next chunk of data."""
        if not self.is_open or self.atEnd():
            return None

        # Fetch more data if buffer is running low
        if len(self.buffer) < max_size:
            try:
                # This needs to run in the async loop's context
                # A more robust implementation would use a thread-safe queue
                # For now, we'll do a blocking call (simplification)
                new_chunk = asyncio.run(self.client.download_media(
                    self.message.media,
                    file=bytes,
                    limit=max(max_size, 128 * 1024), # Fetch at least 128KB
                    offset=self.position + len(self.buffer)
                ))
                if new_chunk:
                    self.buffer.extend(new_chunk)
            except Exception as e:
                self.signals.error.emit(f"خطأ أثناء قراءة البث: {e}")
                return None

        # Return the requested amount of data from the buffer
        data_to_return = bytes(self.buffer[:max_size])
        self.buffer = self.buffer[max_size:]
        self.position += len(data_to_return)

        if not data_to_return and self.atEnd():
            self.signals.finished.emit()

        return data_to_return

    def close(self):
        self.is_open = False
        super().close()