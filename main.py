from pathlib import Path
import sys
import logging

from settings import CFG, setup_logger, update_serial_start
from io_excel import load_input_excel, style_excel, append_and_save, load_person_map
from serial import SerialGenerator
from core import transform

def main():
    logger = setup_logger()
    if len(sys.argv) < 2:
        print("使い方: python main.py <input_excel_path> [--overwrite]")
        sys.exit(1)

    xls_path = Path(sys.argv[1])
    overwrite = "--overwrite" in sys.argv
    logger.info(f"入力ファイル: {xls_path}")
    logger.info(f"強制上書きモード: {'ON' if overwrite else 'OFF'}")

    raw = load_input_excel(xls_path, logger)
    if raw.empty:
        logger.warning("取り込めるデータがありません。")
        return

    old_map = load_person_map(logger)
    used_serials = set(old_map.values())
    serial_gen = SerialGenerator(CFG["serial"], used_serials)
    df_new = transform(raw, serial_gen, old_map)
    append_and_save(df_new, serial_gen, logger, overwrite=overwrite)
    update_serial_start(serial_gen)
    logger.info("=== 完了 ===")

if __name__ == "__main__":
    main()


