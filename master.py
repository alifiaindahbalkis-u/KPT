import argparse

from common.config import WORKER_COUNT_OPTIONS


def main():
    parser = argparse.ArgumentParser(
        description="Jalankan server master pemrosesan dokumen."
    )
    parser.add_argument(
        "--workers",
        "--min-workers",
        dest="worker_target",
        type=int,
        choices=WORKER_COUNT_OPTIONS,
        help="Pilih target worker tanpa prompt interaktif.",
    )
    args = parser.parse_args()

    from master.main import start_master

    start_master(worker_target=args.worker_target)


if __name__ == "__main__":
    main()
