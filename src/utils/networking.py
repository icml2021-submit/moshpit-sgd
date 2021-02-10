import socket
from contextlib import closing
from typing import Optional

Hostname, Port = str, int
Endpoint = str
LOCALHOST = '127.0.0.1'


def get_port(endpoint: Endpoint) -> Optional[Port]:
    try:
        return int(endpoint[endpoint.rindex(':') + 1:], base=10)
    except ValueError:
        return None


def replace_port(endpoint: Endpoint, new_port: Port) -> Endpoint:
    assert endpoint.endswith(':*') or get_port(endpoint) is not None, endpoint
    return f"{endpoint[:endpoint.rindex(':')]}:{new_port}"


def strip_port(endpoint: Endpoint) -> Hostname:
    maybe_port = endpoint[endpoint.rindex(':') + 1:]
    return endpoint[:endpoint.rindex(':')] if maybe_port.isdigit() or maybe_port == '*' else endpoint


def find_open_port(params=(socket.AF_INET, socket.SOCK_STREAM), opt=(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)):
    try:
        with closing(socket.socket(*params))as sock:
            sock.bind(('', 0))
            sock.setsockopt(*opt)
            return sock.getsockname()[1]
    except Exception as e:
        raise e
