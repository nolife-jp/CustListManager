import argparse
from pathlib import Path
import sys

from config.settings import setup_logger, CFG, update_serial_start
from core.excel_io import load_input_excel, style_excel, append_and_save, load_person_map
from core.serial_gen import SerialGenerator
from core.transform import transform

def parse_args():
    parser = argparse.ArgumentParser(description="CustListManager - 顧客リスト管理")
    parser.add_argument("input_excel", help="入力Excelファイルパス")
    parser.add_argument("--overwrite", action="store_true", help="出力ファイルを強制上書き")
    parser.add_argument("--loglevel", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="ログレベル")
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logger(level=args.loglevel)
    logger.info(f"入力ファイル: {args.input_excel}")
    logger.info(f"強制上書きモード: {'ON' if args.overwrite else 'OFF'}")
    xls_path = Path(args.input_excel)
    raw = load_input_excel(xls_path, logger=logger)
    if raw.empty:
        logger.warning("取り込めるデータがありません。")
        return
    old_map = load_person_map(logger=logger)
    used_serials = set(old_map.values())
    serial_gen = SerialGenerator(CFG["serial"], used_serials)
    df_new = transform(raw, serial_gen, old_map)
    append_and_save(df_new, serial_gen, logger=logger, overwrite=args.overwrite)
    update_serial_start(serial_gen)
    logger.info("=== 完了 ===")

if __name__ == "__main__":
    main()
