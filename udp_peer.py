import socket
import time
import threading

UDP_PORT = 9999
BROADCAST_IP = '255.255.255.255'
HEARTBEAT_INTERVAL = 3   # gửi HELLO mỗi 3 giây
PEER_TIMEOUT = 10        # 10 giây không có tin -> offline

# Lưu người dùng theo định dạng:
# { ('192.168.1.5', 5001): {'username': 'UserA', 'last_seen': 12345.6} }
# Khóa là (ip, tcp_port) để nhiều người cùng IP (test trên 1 máy) không đè nhau
online_peers = {}
peer_lock = threading.Lock()

stop_event = threading.Event()   # dùng để tắt sạch tất cả các thread


def get_local_ips():
    """Lấy các IP của chính máy này, dùng để bỏ qua gói do mình tự gửi."""
    ips = {'127.0.0.1'}
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except socket.gaierror:
        pass
    try:
        # connect() với UDP không gửi gói nào, chỉ để hệ điều hành chọn IP đi ra ngoài
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    return ips


def start_broadcaster(my_username, my_tcp_port):
    if '|' in my_username:
        raise ValueError("Username không được chứa ký tự '|'")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    hello = f"HELLO|{my_username}|{my_tcp_port}".encode('utf-8')
    bye = f"BYE|{my_username}|{my_tcp_port}".encode('utf-8')

    while not stop_event.is_set():
        try:
            sock.sendto(hello, (BROADCAST_IP, UDP_PORT))
        except OSError as e:
            print(f"[UDP Lỗi gửi] {e}")
        stop_event.wait(HEARTBEAT_INTERVAL)   # thay cho time.sleep, tắt được ngay

    # Thoát thì báo cho mọi người để họ xóa mình khỏi danh sách ngay
    try:
        sock.sendto(bye, (BROADCAST_IP, UDP_PORT))
    except OSError:
        pass
    sock.close()


def start_listener(my_tcp_port):
    local_ips = get_local_ips()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, 'SO_REUSEPORT'):       # Linux/macOS: cho phép chạy nhiều bản để test
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(('0.0.0.0', UDP_PORT))
    sock.settimeout(1.0)                       # để vòng lặp kiểm tra được stop_event

    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(1024)
        except socket.timeout:
            continue
        except OSError as e:
            print(f"[UDP Lỗi nhận] {e}")
            break

        try:
            parts = data.decode('utf-8').split('|')
            if len(parts) != 3:
                continue
            msg_type, username, port_str = parts
            tcp_port = int(port_str)
        except (UnicodeDecodeError, ValueError):
            continue                           # gói rác -> bỏ qua

        ip = addr[0]

        # Bỏ qua gói do chính mình gửi (broadcast sẽ dội ngược về)
        if ip in local_ips and tcp_port == my_tcp_port:
            continue

        key = (ip, tcp_port)
        with peer_lock:
            if msg_type == "HELLO":
                online_peers[key] = {
                    'username': username,
                    'last_seen': time.monotonic()
                }
            elif msg_type == "BYE":
                online_peers.pop(key, None)

    sock.close()


def check_timeouts():
    while not stop_event.wait(2):              # 2 giây kiểm tra 1 lần
        now = time.monotonic()
        with peer_lock:
            expired = [k for k, d in online_peers.items()
                       if now - d['last_seen'] > PEER_TIMEOUT]
            for k in expired:
                del online_peers[k]


def get_online_peers():
    """Trả về BẢN SAO danh sách, để phần UI/TCP đọc an toàn không cần tự khóa.
    Kết quả: [{'username': 'B', 'ip': '192.168.1.5', 'tcp_port': 5002}, ...]"""
    with peer_lock:
        return [{'username': d['username'], 'ip': k[0], 'tcp_port': k[1]}
                for k, d in online_peers.items()]


def start_discovery(my_username, my_tcp_port):
    """Hàm duy nhất mà phần còn lại của chương trình cần gọi."""
    stop_event.clear()
    for target, args in (
        (start_broadcaster, (my_username, my_tcp_port)),
        (start_listener, (my_tcp_port,)),
        (check_timeouts, ()),
    ):
        threading.Thread(target=target, args=args, daemon=True).start()


def stop_discovery():
    stop_event.set()
    time.sleep(0.3)   # chờ broadcaster kịp gửi BYE


if __name__ == "__main__":
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else "User"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5001

    start_discovery(name, port)
    print(f"[{name}] đang chạy. Ctrl+C để thoát.")
    try:
        while True:
            time.sleep(3)
            print("Online:", get_online_peers())
    except KeyboardInterrupt:
        stop_discovery()