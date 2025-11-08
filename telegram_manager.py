import asyncio
import os
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors.rpcerrorlist import UserAlreadyParticipantError, InviteHashExpiredError, ChannelsTooMuchError
from telethon.tl.types import Channel, Chat

class TelegramManager:
    # --- جديد: تعريف مسار مجلد التخزين المؤقت للصور ---
    CACHE_DIR = "telegram_cache/profile_pics"

    """
    A class to manage interactions with the Telegram API using Telethon.
    It handles fetching dialogs (channels/groups) and messages containing files.
    """
    def __init__(self, api_id, api_hash, session_string):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        # --- جديد: متغير لتتبع حالة الإلغاء ---
        self._cancel_flag = False
        self.client = None
        # --- جديد: التأكد من وجود مجلد التخزين المؤقت ---
        if not os.path.exists(self.CACHE_DIR):
            os.makedirs(self.CACHE_DIR)

    async def _connect(self):
        """Initializes and connects the Telegram client if not already connected."""
        if self.client and self.client.is_connected():
            return

        # Using a new event loop for each async operation in a threaded environment
        # is safer. However, for a manager class, we can try to reuse the client.
        # The loop management will be handled by the calling QRunnable.
        self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
        await self.client.connect()
        if not await self.client.is_user_authorized():
            await self.client.disconnect()
            raise Exception("جلسة Telethon غير صالحة. يرجى تسجيل الدخول مرة أخرى من الإعدادات.")

    async def _disconnect(self):
        """
        Disconnects the client safely, handling potential race conditions and closed loops.
        """
        # This is the most critical fix for the "Event loop is closed" error.
        # We must ensure that we only attempt to disconnect if the client exists,
        # is actually connected, and its event loop is still running.
        if self.client and self.client.is_connected():
            try:
                # Get the loop associated with this client's operations.
                loop = asyncio.get_running_loop()
                if not loop.is_closed():
                    # Gracefully disconnect. This allows Telethon's internal tasks to finish.
                    await self.client.disconnect()
            except Exception as e:
                # If an error occurs during disconnect (e.g., loop already closing), log it but don't crash.
                print(f"Ignoring error during Telethon disconnect: {e}")

    def cancel_current_operation(self):
        """Sets a flag to signal cancellation to any running async loop."""
        self._cancel_flag = True

    async def get_dialogs(self, cancellation_check=None):
        """
        Fetches all dialogs and yields them one by one as a generator.
        """
        await self._connect()
        try:
            async for dialog in self.client.iter_dialogs():
                if self._cancel_flag or (cancellation_check and cancellation_check()):
                    break
                # --- تعديل: التحقق من وجود الصورة في الذاكرة المؤقتة قبل تحميلها ---
                profile_photo_bytes = None
                # اسم ملف الصورة سيكون هو معرّف القناة (ID) لضمان عدم التكرار
                cached_photo_path = os.path.join(self.CACHE_DIR, f"{dialog.id}.jpg")

                if os.path.exists(cached_photo_path):
                    # إذا كانت الصورة موجودة محلياً، اقرأها من الملف
                    with open(cached_photo_path, "rb") as f:
                        profile_photo_bytes = f.read()
                else:
                    # إذا لم تكن موجودة، قم بتحميلها من تيليجرام
                    try:
                        # نستخدم file=bytes لتحميل الصورة في الذاكرة مباشرة
                        downloaded_photo = await self.client.download_profile_photo(dialog.entity, file=bytes)
                        if downloaded_photo:
                            profile_photo_bytes = downloaded_photo
                            # احفظ الصورة في الذاكرة المؤقتة للمرة القادمة
                            with open(cached_photo_path, "wb") as f:
                                f.write(profile_photo_bytes)
                    except Exception:
                        pass # تجاهل الأخطاء إذا لم تكن هناك صورة أو لا يمكن الوصول إليها
                # We are interested in Channels (public/private) and basic group chats.
                if isinstance(dialog.entity, (Channel, Chat)):
                    # Yield the dialog data immediately instead of appending to a list
                    yield {
                        'id': dialog.id,
                        'name': dialog.name,
                        'is_channel': isinstance(dialog.entity, Channel),
                        'profile_photo_bytes': profile_photo_bytes,
                        'username': getattr(dialog.entity, 'username', None)
                    }
        finally:
            await self._disconnect()

    async def get_files_from_dialog(self, dialog_id, cancellation_check=None, progress_callback=None, pause_event=None):
        """
        Fetches all messages with files/photos from a dialog and yields them one by one.
        `cancellation_check`: A callable that returns True to cancel.
        `progress_callback`: A callable to report progress (e.g., how many messages scanned).
        `pause_event`: A threading.Event() to signal pause.
        """
        self._cancel_flag = False # Reset flag at the start of each operation
        await self._connect()
        try:
            # --- إصلاح جذري لمشكلة "Could not find the input entity" ---
            # 1. نحاول الحصول على الكيان مباشرة. هذا يعمل في معظم الحالات.
            try:
                entity = await self.client.get_entity(dialog_id)
            except (ValueError, TypeError):
                # 2. إذا فشل، فهذا يعني أن الكيان غير موجود في ذاكرة Telethon المؤقتة.
                #    لذلك، نقوم بجلب جميع الحوارات (dialogs) والبحث فيها عن الـ ID المطلوب.
                #    هذا يضمن العثور على الكيان طالما أن المستخدم عضو فيه.
                entity = None
                async for dialog in self.client.iter_dialogs():
                    if dialog.id == dialog_id:
                        entity = dialog.entity
                        break
                if not entity:
                    raise ValueError(f"لم يتم العثور على القناة أو المجموعة بالمعرف {dialog_id}. تأكد من أنك عضو فيها.")
            
            message_counter = 0
            # --- إصلاح جذري: استخدام iter_messages لجلب جميع الرسائل من القناة ---
            # هذا يضمن جلب كل شيء من القنوات الكبيرة بدلاً من أول 200 رسالة فقط.
            async for msg in self.client.iter_messages(entity, limit=None): # limit=None لجلب كل الرسائل
                # --- جديد: التحقق من طلب الإلغاء بشكل دوري ---
                # --- جديد: التحقق من حالة الإيقاف المؤقت ---
                if pause_event and pause_event.is_set():
                    while pause_event.is_set():
                        if self._cancel_flag: break # السماح بالإلغاء حتى أثناء الإيقاف المؤقت
                        await asyncio.sleep(0.5) # انتظر قليلاً قبل التحقق مرة أخرى

                message_counter += 1
                if message_counter % 50 == 0:
                    if progress_callback:
                        # Report progress to the main thread
                        progress_callback(message_counter)
                    if self._cancel_flag or (cancellation_check and cancellation_check()):
                        print("File fetching operation cancelled by user.")
                        break # الخروج من الحلقة

                if not (msg.file or msg.photo):
                    continue # تخطي الرسائل التي لا تحتوي على ملفات
                
                media_obj = msg.photo or msg.file
                filename = ""
                
                if msg.photo:
                    filename = f"telegram_photo_{msg.id}.jpg"
                elif msg.file:
                    # استخدام اسم الملف الأصلي إن وجد، وإلا يتم إنشاء اسم افتراضي
                    filename = getattr(msg.file, 'name', None) or f"telegram_file_{msg.id}"
                else:
                    filename = f"telegram_media_{msg.id}"
                
                size = getattr(media_obj, 'size', 0)
                
                # جلب الصورة المصغرة (thumbnail)
                thumbnail_bytes = await self.client.download_media(msg.media, thumb=-1, file=bytes)
                
                # إنشاء رابط داخلي موثوق باستخدام اسم المستخدم أو الـ ID
                channel_ref = getattr(entity, 'username', None) or str(getattr(entity, 'id', ''))
                
                # Yield the file data instead of appending to a list
                yield {
                    'id': msg.id,
                    'title': f"[{msg.date.strftime('%Y-%m-%d')}] {filename}",
                    'filename': filename,
                    'size': size,
                    'url': f"telethon://{channel_ref}/{msg.id}",
                    'thumbnail': thumbnail_bytes
                }
        except Exception as e:
            # If any error occurs during the process, re-raise it to be handled by the worker.
            raise e
        finally:
            await self._disconnect()

    async def join_channel(self, username_or_link):
        """
        Explicitly joins a channel and returns its info upon success.
        """
        await self._connect()
        try:
            # Attempt to join the channel using the provided username or invite link.
            await self.client(JoinChannelRequest(username_or_link))
            # After a successful join, get the entity to return its details.
            entity = await self.client.get_entity(username_or_link)
            return {
                'id': entity.id,
                'name': getattr(entity, 'title', getattr(entity, 'username', 'Unknown')),
                'is_channel': isinstance(entity, Channel)
            }
        except UserAlreadyParticipantError:
            # If already a participant, just get the entity info.
            entity = await self.client.get_entity(username_or_link)
            return {
                'id': entity.id,
                'name': getattr(entity, 'title', getattr(entity, 'username', 'Unknown')),
                'is_channel': isinstance(entity, Channel)
            }
        except (InviteHashExpiredError, ChannelsTooMuchError) as e:
            raise Exception(f"فشل الانضمام للقناة: {e}")
        except Exception as e:
            raise Exception(f"فشل الانضمام أو العثور على القناة '{username_or_link}'. تأكد من صحة الرابط أو المعرف. الخطأ: {e}")
        finally:
            await self._disconnect()

    async def join_and_get_dialog(self, username_or_link):
        """
        The definitive logic to get a channel's info, joining if necessary.
        This is the core logic to solve the user's request.
        """
        await self._connect()
        try:
            try:
                # Step 1: Try to get the entity directly. This works for public channels
                # or channels the user is already a member of.
                entity = await self.client.get_entity(username_or_link)
            except (ValueError, TypeError):
                # Step 2: If get_entity fails, it's likely a private channel we are not in.
                # The most reliable way to handle this is to try and join it.
                try:
                    await self.client(JoinChannelRequest(username_or_link))
                    # If the join is successful, we can now get the entity.
                    entity = await self.client.get_entity(username_or_link)
                except UserAlreadyParticipantError:
                    # This is not an error. We were already in the channel, but Telethon's
                    # cache was stale. We can now safely get the entity.
                    entity = await self.client.get_entity(username_or_link)
                except Exception as join_error:
                    # If joining fails for any other reason (invalid link, banned, etc.), raise that error.
                    raise Exception(f"فشل الانضمام أو العثور على القناة. تأكد من صحة الرابط. الخطأ: {join_error}")

            if entity:
                return {
                    'id': entity.id,
                    'name': getattr(entity, 'title', getattr(entity, 'username', 'Unknown')),
                    'is_channel': isinstance(entity, Channel)
                }
            else:
                return None
                
        except Exception as e:
            # Re-raise the exception to be caught by the QRunnable
            raise e
        finally:
            await self._disconnect()

    async def leave_channel(self, dialog_id):
        """
        Leaves a channel or group.
        """
        await self._connect()
        try:
            # get_entity is needed to resolve the dialog_id to an input peer
            entity = await self.client.get_entity(dialog_id)
            await self.client.delete_dialog(entity)
            return f"تمت مغادرة '{getattr(entity, 'title', 'القناة')}' بنجاح."
        except Exception as e:
            raise Exception(f"فشل مغادرة القناة: {e}")
        finally:
            await self._disconnect()

def run_async_from_sync(coro):
    """
    A helper function to run an async coroutine from a synchronous context (like a QRunnable).
    It ensures a new event loop is created and closed for each operation, which is
    thread-safe.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(coro)
        return result
    finally:
        loop.close()