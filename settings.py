import yaml
import datetime as dt
from pathlib import Path
import logging

CFG_PATH = Path("settings.yaml")
CFG = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))

def setup_logger():
    log_dir = Path(CFG["paths"]["logs_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{dt.datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def update_serial_start(serial_gen):
    # 連番管理用のstart値をYAMLファイルに更新
    import copy
    cfg_copy = copy.deepcopy(CFG)
    cfg_copy["serial"]["start"] = serial_gen.n
    CFG_PATH.write_text(yaml.safe_dump(cfg_copy, allow_unicode=True), encoding="utf-8")
