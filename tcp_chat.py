# tcp_chat.py
# Chạy: python tcp_chat.py <tên> <tcp_port>   (ví dụ: python tcp_chat.py Tai 9107)
# Yêu cầu: udp_peer.py nằm cùng thư mục.

import os
import socket
import sys
import threading

from udp_peer import (start_discovery, stop_discovery,
                           get_online_peers, stop_event)

BUF = 65536
SHOW_PROGRESS_STEP = 10     # in tiến độ mỗi 10%

peers = {}                  # key (ip, tcp_port) -> Connection
peers_lock = threading.Lock()
current = None              # Connection đang chat
unread = {}                 # tên -> số tin chưa đọc
last_list = []              # kết quả /list gần nhất, để chọn theo số
my_name = None
my_port = None

print_lock = threading.Lock()


def say(msg):
    with print_lock:
        print(msg)


class Connection:
    def __init__(self, sock, key, name):
        self.sock = sock
        self.key = key              # (ip, tcp_port) khớp với UDP
        self.name = name
        self.send_lock = threading.Lock()


# ---------- Đọc / ghi an toàn trên TCP stream ----------

def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(BUF, n - len(buf)))
        if not chunk:
            raise ConnectionError("peer đã đóng kết nối")
        buf.extend(chunk)
    return bytes(buf)


def recv_field(sock):
    """Đọc từng byte đến dấu '|' (dùng cho header)."""
    buf = bytearray()
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("peer đã đóng kết nối")
        if b == b"|":
            return buf.decode("utf-8")
        buf.extend(b)


def send_raw(conn, data):
    with conn.send_lock:
        conn.sock.sendall(data)


# ---------- Quản lý kết nối ----------

def drop(conn, notify=False):
    global current
    with peers_lock:
        if peers.get(conn.key) is conn:
            del peers[conn.key]
    if current is conn:
        current = None
    try:
        conn.sock.close()
    except OSError:
        pass
    if notify:
        say(f"[*] {conn.name} đã ngắt kết nối")


def on_message(conn, text):
    if conn is current:
        say(f"[{conn.name}] {text}")
    else:
        unread[conn.name] = unread.get(conn.name, 0) + 1
        say(f"[*] Tin mới từ {conn.name} ({unread[conn.name]} chưa đọc)")


def receiver(conn):
    """Một thread cho mỗi kết nối chat: nhận MSG / BYE."""
    try:
        while not stop_event.is_set():
            kind = recv_field(conn.sock)
            if kind == "MSG":
                length = int(recv_field(conn.sock))
                text = recv_exact(conn.sock, length).decode("utf-8", "replace")
                on_message(conn, text)
            elif kind == "BYE":
                break
            else:
                raise ValueError(f"gói không hợp lệ: {kind}")
    except (OSError, ValueError):
        pass
    finally:
        drop(conn, notify=True)


# ---------- Nhận file ----------

def recv_file(sock):
    name = os.path.basename(recv_field(sock)) or "file"   # tránh path traversal
    size = int(recv_field(sock))
    os.makedirs("downloads", exist_ok=True)
    tmp = os.path.join("downloads", name + ".part")
    final = os.path.join("downloads", name)
    say(f"[*] Nhận {name} ({size} bytes)...")
    try:
        with open(tmp, "wb") as f:
            got, last = 0, -SHOW_PROGRESS_STEP
            while got < size:
                chunk = sock.recv(min(BUF, size - got))
                if not chunk:
                    raise ConnectionError("truyền file bị gián đoạn")
                f.write(chunk)
                got += len(chunk)
                pct = got * 100 // size if size else 100
                if pct >= last + SHOW_PROGRESS_STEP or got == size:
                    say(f"    {name}: {pct}%")
                    last = pct
        os.replace(tmp, final)
        say(f"[*] Đã lưu {final}")
    except (OSError, ValueError) as e:
        if os.path.exists(tmp):
            os.remove(tmp)                  # xóa file dở dang
        say(f"[!] Lỗi nhận file {name}: {e}")


# ---------- Server TCP ----------

def handle_incoming(sock, addr):
    try:
        sock.settimeout(5)
        kind = recv_field(sock)

        if kind == "HI":                    # kết nối chat
            name = recv_field(sock)
            port = int(recv_field(sock))
            sock.settimeout(None)
            conn = Connection(sock, (addr[0], port), name)
            with peers_lock:
                peers[conn.key] = conn
            say(f"[*] {name} đã kết nối chat với bạn")
            receiver(conn)                  # receiver tự đóng socket khi xong

        elif kind == "FILE":                # kết nối truyền file riêng
            sock.settimeout(30)
            try:
                recv_file(sock)
            finally:
                sock.close()

        else:
            sock.close()
    except (OSError, ValueError):
        try:
            sock.close()
        except OSError:
            pass


def start_server(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(5)
    srv.settimeout(1.0)

    def loop():
        while not stop_event.is_set():
            try:
                sock, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=handle_incoming, args=(sock, addr),
                             daemon=True).start()
        srv.close()

    threading.Thread(target=loop, daemon=True).start()
    say(f"[*] Đang nghe TCP trên port {port}")


# ---------- Phía gửi ----------

def open_chat(peer):
    key = (peer["ip"], peer["tcp_port"])
    with peers_lock:
        if key in peers:
            return peers[key]               # đã có kết nối, tái sử dụng

    sock = socket.create_connection(key, timeout=3)
    sock.settimeout(None)
    conn = Connection(sock, key, peer["username"])
    send_raw(conn, f"HI|{my_name}|{my_port}|".encode("utf-8"))
    with peers_lock:
        peers[key] = conn
    threading.Thread(target=receiver, args=(conn,), daemon=True).start()
    return conn


def send_msg(conn, text):
    data = text.encode("utf-8")             # length tính theo byte
    send_raw(conn, f"MSG|{len(data)}|".encode("utf-8") + data)


def send_file(peer_key, path):
    name = os.path.basename(path)
    size = os.path.getsize(path)
    try:
        with socket.create_connection(peer_key, timeout=5) as s:
            s.settimeout(30)
            s.sendall(f"FILE|{name}|{size}|".encode("utf-8"))
            with open(path, "rb") as f:
                sent, last = 0, -SHOW_PROGRESS_STEP
                while chunk := f.read(BUF):
                    s.sendall(chunk)
                    sent += len(chunk)
                    pct = sent * 100 // size if size else 100
                    if pct >= last + SHOW_PROGRESS_STEP or sent == size:
                        say(f"    gửi {name}: {pct}%")
                        last = pct
        say(f"[*] Đã gửi {name} xong")
    except OSError as e:
        say(f"[!] Gửi file {name} lỗi: {e}")


# ---------- Lệnh người dùng ----------

def resolve(arg):
    if arg.isdigit():
        i = int(arg) - 1
        return last_list[i] if 0 <= i < len(last_list) else None
    for p in last_list:
        if p["username"] == arg:
            return p
    return None


def cmd_list():
    global last_list
    last_list = sorted(get_online_peers(), key=lambda p: p["username"].lower())
    if not last_list:
        say("(chưa có ai online)")
    for i, p in enumerate(last_list, 1):
        say(f"{i}. {p['username']} ({p['ip']}:{p['tcp_port']})")


def cmd_chat(arg):
    global current
    if not arg:
        say("Dùng: /chat <tên|số>")
        return
    p = resolve(arg)
    if p is None and not arg.isdigit():
        cmd_list()                          # thử làm mới danh sách rồi tìm lại
        p = resolve(arg)
    if p is None:
        say("[!] Không tìm thấy người này. Gõ /list trước.")
        return
    try:
        conn = open_chat(p)
    except OSError as e:
        say(f"[!] Không kết nối được {p['username']}: {e}")
        return
    current = conn
    unread.pop(conn.name, None)
    say(f"[*] Đã kết nối {conn.name} ({conn.key[0]}:{conn.key[1]})")


def cmd_leave():
    global current
    if current is None:
        say("Bạn chưa chat với ai.")
        return
    conn = current
    try:
        send_raw(conn, b"BYE|")
    except OSError:
        pass
    drop(conn)
    say(f"[*] Đã đóng chat với {conn.name}")


def cmd_send(arg):
    if current is None:
        say("Hãy /chat với ai đó trước khi gửi file.")
        return
    if not arg or not os.path.isfile(arg):
        say("[!] File không tồn tại.")
        return
    threading.Thread(target=send_file, args=(current.key, arg),
                     daemon=True).start()


def cmd_help():
    say("/list                  xem người online\n"
        "/chat <tên|số>         mở chat\n"
        "/send <file>           gửi file cho người đang chat\n"
        "/leave                 đóng chat hiện tại\n"
        "/help                  xem lệnh\n"
        "/quit                  thoát\n"
        "<nội dung>             gửi tin nhắn cho người đang chat")


def cmd_quit():
    with peers_lock:
        conns = list(peers.values())
    for conn in conns:
        try:
            send_raw(conn, b"BYE|")
        except OSError:
            pass
        drop(conn)
    stop_discovery()


def handle_line(line):
    if not line:
        return True
    if not line.startswith("/"):
        if current is None:
            say("Chưa chat với ai. Dùng /chat <tên|số>")
            return True
        try:
            send_msg(current, line)
        except OSError as e:
            say(f"[!] Gửi thất bại: {e}")
            drop(current, notify=True)
        return True

    parts = line.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/list":
        cmd_list()
    elif cmd == "/chat":
        cmd_chat(arg)
    elif cmd == "/leave":
        cmd_leave()
    elif cmd == "/send":
        cmd_send(arg)
    elif cmd == "/help":
        cmd_help()
    elif cmd == "/quit":
        cmd_quit()
        return False
    else:
        say("Lệnh không hợp lệ. Gõ /help để xem.")
    return True


def main():
    global my_name, my_port
    my_name = sys.argv[1] if len(sys.argv) > 1 else "User"
    my_port = int(sys.argv[2]) if len(sys.argv) > 2 else 5001

    start_server(my_port)                   # mở TCP trước khi quảng bá
    start_discovery(my_name, my_port)
    say(f"[*] Chạy với tên {my_name}. Gõ /help để xem lệnh.")

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            line = "/quit"
        if not handle_line(line):
            break


if __name__ == "__main__":
    main()