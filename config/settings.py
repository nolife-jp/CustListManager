import logging
from pathlib import Path
import datetime as dt

CFG_PATH = Path("settings.yaml")
import yaml
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

def setup_logger(level="INFO"):
    log_dir = Path(CFG["paths"]["logs_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{dt.datetime.now():%Y%m%d_%H%M%S}.log"
    logger = logging.getLogger("CustListManager")
    logger.handlers.clear()  # 二重登録防止
    logger.setLevel(level.upper())
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def update_serial_start(serial_gen):
    import copy
    cfg_copy = copy.deepcopy(CFG)
    cfg_copy["serial"]["start"] = serial_gen.n
    CFG_PATH.write_text(yaml.safe_dump(cfg_copy, allow_unicode=True), encoding="utf-8")
