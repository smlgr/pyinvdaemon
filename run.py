import datetime
import logging.config
import signal
import socket
import threading
import time
from queue import Queue

import requests

SOCKET_BUFFER_SIZE = 8192

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "stderr": {
            "level": logging.DEBUG,
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr"
        },
    },
    "loggers": {
        "": {
            "handlers": ["stderr"],
            "level": logging.DEBUG,
            "propagate": False
        }
    }
})

_logger = logging.getLogger(__name__)


class PyInvDaemon:

    def __init__(self):
        signal.signal(signal.SIGINT, self._signal_handler)

        self._thread_inverter = threading.Thread(target=self.loop_inverter)
        self._thread_server = threading.Thread(target=self.loop_server)

        self._queue = Queue()

        self._keep_running = True
        self._loop_wait = 1

    def _signal_handler(self, sig, frame):
        if sig == signal.SIGINT:
            self.stop()

    def start(self):
        _logger.info("START")
        self._thread_inverter.start()
        self._thread_server.start()

    def stop(self):
        _logger.info("STOP")
        self._keep_running = False

    def join(self):
        _logger.info("JOIN")
        self._thread_inverter.join()
        self._thread_server.join()

    def loop_inverter(self):
        while self._keep_running:
            _logger.info("LOOP")

            try:
                data_request = self._prepare_data_request()
                _logger.debug("Request: %s" % data_request)

                data_response = self._call_inverter("172.16.83.2", 12345, data_request)
                _logger.debug("Response: %s" % data_response)

                data = self._data_parse(data_response)
                _logger.debug("Data: %s" % data)

                self._save_data(data)
            except Exception as e:
                _logger.warning(e)

            time.sleep(self._loop_wait)

    def loop_server(self):
        while self._keep_running:
            request_body = []

            while not self._queue.empty():
                data_body = self._queue.get()
                request_body.append(data_body)

            if len(request_body) > 0:
                res = requests.post(
                    url="https://smlgr.thehellnet.org/api/public/v1/data",
                    json=request_body
                )

                if res.status_code != 200:
                    _logger.warning(res)

            time.sleep(self._loop_wait)

    def _data_parse(self, data_response):
        if not data_response.startswith("{") or not data_response.endswith("}"):
            raise ValueError("Malformed data to parse")

        data = {}

        items = data_response.split("|")[1][3:].split(";")

        for item in items:
            key, value = item.split("=")
            data[key] = int(value, 16)

        return data

    def _prepare_data_request(self):
        query = "UDC;IDC;UL1;IL1;PAC;PRL;TKK;TNF;KDY;KLD"
        ln = 13 + len(query) + 6
        tmp = "%02X;%02X;%02X|64:%s|" % (0xFB, 0x01, ln, query)
        checksum = self._checksum16(tmp)
        data_request = "{%s%04X}" % (tmp, checksum)
        return data_request

    def _call_inverter(self, host, port, data):
        sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sck.settimeout(1)
        sck.connect((host, port))
        sck.send(data.encode())
        data_response = sck.recv(SOCKET_BUFFER_SIZE)
        sck.close()
        return data_response.decode()

    def _save_data(self, data):
        data_body = {
            "ts": datetime.datetime.now(),
            "dc_voltage": float(data["UDC"] / 10),
            "dc_current": float(data["IDC"] / 100),
            "ac_voltage": float(data["UL1"] / 10),
            "ac_current": float(data["IL1"] / 100),
            "power": float(data["PAC"] / 10),
            "frequency": float(data["TNF"] / 100)
        }

        self._queue.put(data_body)

    def _checksum16(self, string):
        cs = 0

        for c in string:
            cs += ord(c)
            cs %= 0xFFFF

        return cs


if __name__ == "__main__":
    py_inv_daemon = PyInvDaemon()
    py_inv_daemon.start()
