"""One-shot public market handshake probe. Manual diagnostics only; stdlib only.

Usage: python3 scripts/pm_ws_compression_probe.py --asset-id PUBLIC_TOKEN_ID
Sends one public subscription then a close, without decoding market data.
Never reads environment/configuration or uses authentication. No retry loop.
"""
import argparse
import base64
import hashlib
import json
import os
import socket
import ssl
import struct

HOST = 'ws-subscriptions-clob.polymarket.com'
PATH = '/ws/market'


def masked_frame(opcode, payload):
    mask = os.urandom(4)
    length = len(payload)
    header = bytes([0x80 | opcode, 0x80 | length]) if length < 126 else bytes([0x80 | opcode, 0xfe]) + struct.pack('!H', length)
    return header + mask + bytes(value ^ mask[i % 4] for i, value in enumerate(payload))


def probe(asset_id, timeout=8):
    if not asset_id.isascii() or not asset_id.isdecimal() or len(asset_id) > 100:
        raise ValueError('asset-id must be a public numeric outcome ID (1-100 digits)')
    key = base64.b64encode(os.urandom(16)).decode('ascii')
    request = (f'GET {PATH} HTTP/1.1\r\nHost: {HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
               f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
               'Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n')
    with (socket.create_connection((HOST, 443), timeout=timeout) as tcp,
          ssl.create_default_context().wrap_socket(tcp, server_hostname=HOST) as conn):
        conn.sendall(request.encode('ascii'))
        response = b''
        while b'\r\n\r\n' not in response:
            chunk = conn.recv(4096)
            if not chunk or len(response) + len(chunk) > 16384:
                raise ValueError('incomplete or oversized handshake')
            response += chunk
        lines = response.split(b'\r\n\r\n', 1)[0].decode('iso-8859-1').split('\r\n')
        headers = {k.lower(): v.strip() for line in lines[1:] for k, v in [line.split(':', 1)]}
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode('ascii')).digest()).decode('ascii')
        if (lines[0].split()[1] != '101' or headers.get('sec-websocket-accept') != accept
                or headers.get('upgrade', '').lower() != 'websocket'):
            raise ValueError('server did not accept WebSocket upgrade')
        # Even with negotiated compression, uncompressed client frames are
        # valid. No PING or other WS message precedes this subscription.
        conn.sendall(masked_frame(1, json.dumps({'type': 'market', 'assets_ids': [asset_id]}).encode('ascii')))
        conn.sendall(masked_frame(8, struct.pack('!H', 1000)))
        extension = headers.get('sec-websocket-extensions', '')
        return {'permessage_deflate_accepted': any(
            part.split(';', 1)[0].strip().lower() == 'permessage-deflate' for part in extension.split(',')),
            'handshake_headers': {k: v[:256] for k, v in headers.items()
                if k in {'sec-websocket-extensions', 'cf-ray', 'server', 'date'}}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--asset-id', required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(probe(args.asset_id), sort_keys=True))
    except (OSError, ValueError):
        # Do not dump arbitrary response bodies/headers on failure.
        parser.exit(1, 'Public WebSocket compression probe failed (network or invalid upgrade).\n')


if __name__ == '__main__':
    main()
