"""
管理番号発行（SerialGeneratorクラス）
"""
import random

class SerialGenerator:
    def __init__(self, cfg: dict, used: set[str]):
        self.prefix = cfg["prefix"]
        self.digits = cfg["digits"]
        self.charset = cfg["random_suffix"]["charset"]
        self.rand_len = cfg["random_suffix"]["length"]
        self.n = cfg["start"]
        self.used = used

    def next(self) -> str:
        while True:
            serial = (
                f"{self.prefix}{str(self.n).zfill(self.digits)}"
                f"{''.join(random.choice(self.charset) for _ in range(self.rand_len))}"
            )
            self.n += 1
            if serial not in self.used:
                self.used.add(serial)
                return serial

    def get_serial(self, name, mail):
        # nameとmailから決定的なserialを返したい場合はここに実装
        return self.next()
