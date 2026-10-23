import random
import re
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from scraper.accounts.stream import stream_reel

DATA = bytes(range(256)) * 400          # 102,400 bytes pretending to be a 20 second Reel
DURATION = 20.0


class RangeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        m = re.match(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
        start, end = (int(m.group(1)), min(int(m.group(2)), len(DATA) - 1)) if m else (0, len(DATA) - 1)
        body = DATA[start:end + 1]
        self.send_response(206 if m else 200)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class StreamReelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), RangeHandler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}/reel.mp4"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_mix_of_full_half_and_skipped_views(self):
        rng = random.Random(7)
        fractions = [stream_reel(self.url, DURATION, "", rng, sleep=lambda s: None)["fraction"] for _ in range(60)]
        self.assertTrue(any(f >= 0.99 for f in fractions), "some Reels are watched to the end")
        self.assertTrue(any(0.35 <= f <= 0.75 for f in fractions), "some are watched about half")
        self.assertTrue(any(f <= 0.35 for f in fractions), "some are skipped early")

    def test_bytes_follow_the_fraction_and_sleep_follows_playback(self):
        slept = []
        result = stream_reel(self.url, DURATION, "", random.Random(1), sleep=slept.append)
        self.assertAlmostEqual(result["bytes"] / len(DATA), result["fraction"], delta=0.05)
        self.assertAlmostEqual(sum(slept), result["fraction"] * DURATION, delta=result["fraction"] * DURATION * 0.12 + 1)

    def test_missing_video_url_or_duration_is_an_error(self):
        with self.assertRaises(ValueError):
            stream_reel("", DURATION)
        with self.assertRaises(ValueError):
            stream_reel(self.url, None)


if __name__ == "__main__":
    unittest.main()
