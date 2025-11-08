import sys
import json
import struct

# --- هذا السكربت يعمل كوسيط بين إضافة كروم وبرنامجك الرئيسي ---

# ملاحظة: هذا السكربت يجب أن يكون قابلاً للتشغيل باستخدام بايثون
# تأكد من أن بايثون موجود في متغيرات البيئة (PATH)

def get_message():
    """
    يقرأ رسالة واحدة (JSON) من الإدخال القياسي (stdin) كما يرسلها كروم.
    """
    # أول 4 بايت تحتوي على طول الرسالة
    raw_length = sys.stdin.buffer.read(4)
    if len(raw_length) == 0:
        sys.exit(0)
    message_length = struct.unpack('@I', raw_length)[0]
    message = sys.stdin.buffer.read(message_length).decode('utf-8')
    return json.loads(message)

def send_message(message_dict):
    """
    يرسل رسالة (JSON) إلى الإخراج القياسي (stdout) لترجع إلى كروم.
    """
    message = json.dumps(message_dict)
    # يجب أن نسبق الرسالة بطولها (4 بايت)
    sys.stdout.buffer.write(struct.pack('@I', len(message)))
    sys.stdout.buffer.write(message.encode('utf-8'))
    sys.stdout.buffer.flush()

if __name__ == '__main__':
    try:
        # 1. استقبال الرابط من إضافة كروم
        received_message = get_message()
        url_to_download = received_message.get("url")

        if url_to_download:
            # 2. إرسال الرابط إلى برنامجك الرئيسي باستخدام QLocalSocket
            # (نفس الآلية التي يستخدمها بروتوكول mostafa-dl://)
            from PySide6.QtNetwork import QLocalSocket
            socket = QLocalSocket()
            socket.connectToServer("MostafaDownloaderInstance")
            if socket.waitForConnected(500):
                socket.write(url_to_download.encode('utf-8'))
                socket.waitForBytesWritten(500)
            socket.disconnectFromServer()

        # 3. (اختياري) إرسال رد إلى الإضافة لتأكيد الاستلام
        send_message({"status": "success", "received_url": url_to_download})

    except Exception as e:
        # في حالة حدوث خطأ، يمكننا إرساله إلى الإضافة لتصحيح الأخطاء
        send_message({"status": "error", "message": str(e)})