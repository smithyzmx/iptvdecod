from flask import Flask, request, Response, stream_with_context
import requests
import json
import socket
import threading
import time
from datetime import datetime
from urllib.parse import urlparse, urljoin, quote, unquote
from collections import deque
import re
import logging

app = Flask(__name__)

# ==================== CONFIGURAÇÕES GLOBAIS OTIMIZADAS ====================
TARGET = 'example'
PROXY_HOST = "0.0.0.0"
PROXY_PORT = 8000  # Mantido na porta 8000 original
BUFFER_SIZE = 65536  
CACHE_DURATION_SECONDS = 5
MAX_RETRIES = 5
RETRY_DELAY = 0.5

# Sessão Persistente (Reaproveita conexões TCP)
http_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
http_session.mount('http://', adapter)
http_session.mount('https://', adapter)

CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

LISTAS_M3U = [
    'https://listas.oneplayhd.com/lista01.txt',
    'https://listas.oneplayhd.com/lista02.txt',
    'https://listas.oneplayhd.com/lista03.txt',
    'https://listas.oneplayhd.com/lista04.txt',
    'https://listas.oneplayhd.com/lista05.txt',
    'https://listas.oneplayhd.com/lista06.txt',
    'https://listas.oneplayhd.com/lista07.txt',
    'https://listas.oneplayhd.com/lista08.txt'
]

SERVER_CACHE = {}
SERVER_CACHE_EXPIRY = 600

def extrair_credenciais_xtream(conteudo_m3u):
    urls = re.findall(r'(https?://[^\s]+\.m3u8?)', conteudo_m3u)
    if not urls: return None
    
    parsed = urlparse(urls[0])
    host_completo = parsed.netloc
    partes = parsed.path.strip('/').split('/')
    
    username, password = None, None
    if len(partes) >= 3 and not any(p in partes for p in ['live', 'movie', 'series']):
        username, password = partes[0], partes[1]
    elif 'live' in partes:
        idx = partes.index('live')
        if len(partes) > idx + 2: username, password = partes[idx + 1], partes[idx + 2]
    elif 'movie' in partes or 'series' in partes:
        tipo = 'movie' if 'movie' in partes else 'series'
        idx = partes.index(tipo)
        if len(partes) > idx + 2: username, password = partes[idx + 1], partes[idx + 2]
            
    if not username or not password:
        match = re.search(r'x-tvg-url="[^"]*username=([^&]+)&password=([^"]+)"', conteudo_m3u)
        if match: username, password = match.group(1), match.group(2)
            
    return {
        'host_base': f"{parsed.scheme}://{host_completo}",
        'username': username,
        'password': password
    }

def select_server(options):
    now = time.time()
    if options in SERVER_CACHE and (now - SERVER_CACHE[options]['time']) < SERVER_CACHE_EXPIRY:
        return SERVER_CACHE[options]['data']

    try:
        r = http_session.get(LISTAS_M3U[options], headers={'User-Agent': CHROME_UA}, timeout=8)
        dados = extrair_credenciais_xtream(r.text)
        if dados: SERVER_CACHE[options] = {'time': now, 'data': dados}
        return dados
    except:
        return SERVER_CACHE.get(options, {}).get('data')

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except: return '127.0.0.1'

LOCAL_IP = get_local_ip()

# ==================== CACHE DE MEMÓRIA ====================
class CircularBuffer:
    def __init__(self, max_seconds=5, max_chunks=250):
        self.buffer = deque(maxlen=max_chunks)
        self.timestamps = deque(maxlen=max_chunks)
        self.max_seconds = max_seconds
        self.total_bytes = 0
        self.lock = threading.Lock()

    def add_chunk(self, chunk):
        with self.lock:
            self.buffer.append(chunk)
            self.timestamps.append(time.time())
            self.total_bytes += len(chunk)
            cutoff = time.time() - self.max_seconds
            while self.timestamps and self.timestamps[0] < cutoff:
                self.total_bytes -= len(self.buffer.popleft())
                self.timestamps.popleft()

    def get_recovery_chunks(self, duration=3):
        with self.lock:
            if not self.buffer: return []
            cutoff = time.time() - duration
            recovery = [self.buffer[i] for i, ts in enumerate(self.timestamps) if ts >= cutoff]
            return recovery if recovery else list(self.buffer)[-20:]

    def get_continuous_chunks(self, count=30):
        with self.lock: return list(self.buffer)[-count:] if self.buffer else []

# ==================== PROXY UNIFICADO ====================
class UnifiedStreamProxy:
    def __init__(self):
        self.channel_caches = {}
        self.stream_lock = threading.Lock()

    def get_channel_cache(self, url):
        clean_url = re.sub(r'(_=\d+|timestamp=\d+|t=\d+|seq=\d+)', '', url)
        with self.stream_lock:
            if clean_url not in self.channel_caches:
                self.channel_caches[clean_url] = CircularBuffer(CACHE_DURATION_SECONDS)
            return self.channel_caches[clean_url]

    def fetch_with_session(self, url, headers=None, range_header=None):
        req_headers = {'User-Agent': CHROME_UA}
        if headers:
            for k, v in headers.items():
                if k.lower() not in ['host', 'connection', 'content-length', 'range', 'accept-encoding']:
                    req_headers[k] = v
        if range_header: req_headers['Range'] = range_header
        
        for attempt in range(MAX_RETRIES):
            try:
                resp = http_session.get(url, headers=req_headers, stream=True, timeout=15)
                if resp.status_code in [200, 206]:
                    return resp, resp.status_code
            except Exception:
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY)
        return None, 0

    def handle_channel_stream(self, url, headers):
        cache = self.get_channel_cache(url)
        response, status_code = self.fetch_with_session(url, headers)
        
        if not response:
            try:
                for chunk in cache.get_recovery_chunks():
                    yield chunk
                    time.sleep(0.01)
            except GeneratorExit:
                return
            return
            
        try:
            content_type = response.headers.get('content-type', '').lower()
            if 'mpegurl' in content_type or '.m3u8' in url.lower():
                texto = response.content.decode('utf-8', errors='ignore')
                linhas = []
                base_url = response.url.rsplit('/', 1)[0]
                proxy_base = f"http://{LOCAL_IP}:{PROXY_PORT}"
                
                for linha in texto.split('\n'):
                    linha = linha.strip()
                    if linha and not linha.startswith('#') and not linha.startswith(proxy_base):
                        try:
                            abs_url = urljoin(base_url + '/', linha)
                            linha = f"{proxy_base}/?url={quote(abs_url, safe='')}"
                        except Exception: pass
                    linhas.append(linha)
                    
                response.close()
                yield '\n'.join(linhas).encode('utf-8')
                return
                
            consec_err = 0
            while True:
                try:
                    if response:
                        chunk = response.raw.read(BUFFER_SIZE)
                        if chunk:
                            cache.add_chunk(chunk)
                            yield chunk
                            consec_err = 0
                        else: break
                    else:
                        for chunk in cache.get_continuous_chunks(30):
                            yield chunk
                            time.sleep(0.01)
                        new_resp, st = self.fetch_with_session(url, headers, f"bytes={cache.total_bytes}-")
                        if new_resp and st in [200, 206]: response = new_resp
                        else: time.sleep(1)
                except GeneratorExit:
                    # CLIENTE DESCONECTOU (MUDOU DE CANAL) - FECHA TUDO EM SILÊNCIO
                    break
                except Exception:
                    consec_err += 1
                    if consec_err >= 3:
                        try:
                            if response: response.close()
                            response, st = self.fetch_with_session(url, headers, f"bytes={cache.total_bytes}-")
                            if st in [200, 206]: consec_err = 0
                        except Exception: pass
                    try:
                        for chunk in cache.get_continuous_chunks(20):
                            yield chunk
                            time.sleep(0.01)
                    except GeneratorExit:
                        break
        finally:
            if response:
                try: response.close()
                except Exception: pass

    def handle_mp4_stream_response(self, url, method, req_headers):
        headers = {k: v for k, v in req_headers.items() if k.lower() not in ['host', 'connection', 'accept-encoding']}
        headers['Accept-Encoding'] = 'identity'
        if range_header := req_headers.get('range') or req_headers.get('Range'):
            headers['Range'] = range_header

        response = None
        for attempt in range(MAX_RETRIES):
            try:
                response = http_session.request(method=method, url=url, headers=headers, stream=True, timeout=15)
                if response.status_code in [200, 206]: break
            except Exception:
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY)

        if not response or response.status_code not in [200, 206]:
            return Response("Video indisponível", status=502)

        resp_headers = {k: v for k, v in response.headers.items() if k.lower() in ['content-type', 'content-length', 'content-range']}
        resp_headers.update({'Access-Control-Allow-Origin': '*'}) 

        @stream_with_context
        def generate():
            try:
                for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
                    if chunk: yield chunk
            except GeneratorExit:
                pass # Cliente saiu, finaliza de forma limpa
            finally:
                response.close()
                
        return Response(generate(), status=response.status_code, headers=resp_headers)

    def _detect_mp4(self, url):
        l = url.lower()
        return any(x in l for x in ['.mp4','.mkv','.avi']) or '/play/' in l or ('xtream' in l and ('/movie/' in l or '/series/' in l))

stream_proxy = UnifiedStreamProxy()

def extract_url_from_path(path):
    if '/?url=' in path: return unquote(path.split('/?url=', 1)[1].split('&', 1)[0])
    if '/tsdownloader' in path and '?url=' in path:
        for p in path.split('?', 1)[1].split('&'):
            if p.startswith('url='): return unquote(p[4:])
    if path.startswith(('/http://', '/https://')): return unquote(path[1:])
    if path.startswith(('http://', 'https://')): return unquote(path)
    return None

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'POST', 'OPTIONS'])
def proxy(path):
    url = request.args.get('url') if (path == '' and request.args.get('url')) else extract_url_from_path(path)
    
    if url:
        headers_dict = dict(request.headers)
        if stream_proxy._detect_mp4(url):
            return stream_proxy.handle_mp4_stream_response(url, request.method, headers_dict)
        return Response(stream_with_context(stream_proxy.handle_channel_stream(url, headers_dict)), 
                        content_type='application/vnd.apple.mpegurl' if '.m3u8' in url.lower() else 'video/mp2t',
                        headers={'Access-Control-Allow-Origin': '*'})

    target_url = path if path.startswith(('http://', 'https://')) else f"{TARGET}/{path}"
    if request.query_string: target_url += "?" + request.query_string.decode()
    
    username, password = "user", "pass"
    
    for i in range(1, 9):
        lista_name = f'lista{i}'
        if lista_name in target_url.lower():
            dados = select_server(i-1)
            if dados:
                target_url = target_url.replace('example', dados.get('host_base', '')) \
                                       .replace(lista_name, dados.get('username', '')) \
                                       .replace('12345', dados.get('password', ''))
                username, password = lista_name, '12345'
            break

    if any(s in path for s in ['live/', '/live/', 'movie/', '/movie/', 'series/', '/series/']):
        headers_dict = dict(request.headers)
        if stream_proxy._detect_mp4(target_url):
            return stream_proxy.handle_mp4_stream_response(target_url, request.method, headers_dict)
        return Response(stream_with_context(stream_proxy.handle_channel_stream(target_url, headers_dict)),
                        content_type='application/vnd.apple.mpegurl' if '.m3u8' in target_url.lower() else 'video/mp2t',
                        headers={'Access-Control-Allow-Origin': '*'})

    headers = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'accept-encoding']}
    headers['Accept-Encoding'] = 'identity'
    
    try:
        resp = http_session.request(method=request.method, url=target_url, headers=headers, data=request.get_data(), timeout=15)
        content = resp.content
        
        if 'player_api.php' in path:
            try:
                data = json.loads(content.decode('utf-8', errors='ignore').strip())
                if 'user_info' in data:
                    data['user_info']['username'] = username
                    data['user_info']['password'] = password
                data['server_info'] = {'url': f"http://{LOCAL_IP}:{PROXY_PORT}", 'port': PROXY_PORT}
                content = json.dumps(data).encode('utf-8')
            except Exception: pass
            
        excluded = {'content-encoding', 'content-length', 'transfer-encoding', 'connection'}
        response_headers = [(k, v) for k, v in resp.headers.items() if k.lower() not in excluded]
        if 'player_api.php' in path:
            response_headers = [(k, v) if k.lower() != 'content-type' else ('Content-Type', 'application/json') for k, v in response_headers]
            
        return Response(content, status=resp.status_code, headers=response_headers)
    except Exception as e:
        return {"success": False, "error": str(e)}, 500

@app.route('/health', methods=['GET'])
def health():
    return {"status": "ok", "proxy": f"http://{LOCAL_IP}:{PROXY_PORT}"}

def run_service():
    print(f"""
    ╔═══════════════════════════════════════════════╗
    ║  🔥 FREE IPTV PROXY (ARQUITETURA HÍBRIDA) 🔥  ║
    ╠═══════════════════════════════════════════════╣
    ║  HOST LOCAL: http://{LOCAL_IP}:{PROXY_PORT:<22} ║
    ╚═══════════════════════════════════════════════╝
    """)
    try:
        from waitress import serve
        print("🚀 Servidor Waitress de alta performance ativado!")
        print("⚡ Sessões TCP HTTP Persistentes ativadas!")
        print("🤫 Modo Silencioso: Logs de streaming omitidos para evitar travamentos.")
        print("🟢 Aguardando conexões...\n")
        
        # Desliga logs agressivos do Waitress
        logger = logging.getLogger('waitress')
        logger.setLevel(logging.CRITICAL)
        
        serve(app, host="0.0.0.0", port=PROXY_PORT, threads=16)
    except ImportError:
        print("⚠️ AVISO: Usando Flask padrão. Para evitar travadas, instale:")
        print("👉 pip install waitress")
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)
        app.run(host="0.0.0.0", port=PROXY_PORT, threaded=True, debug=False, use_reloader=False)

if __name__ == '__main__':
    run_service()
