import argparse
import os


def main():
    parser = argparse.ArgumentParser(
        description="Jalankan server master pemrosesan dokumen."
    )
    parser.add_argument(
        "--min-workers",
        type=int,
        default=int(os.environ.get("MIN_WORKERS", "2")),
        help="Jumlah worker minimum sebelum tugas dibagikan (default: 2).",
    )
    args = parser.parse_args()

    if args.min_workers < 1:
        parser.error("--min-workers harus minimal 1.")

    os.environ["MIN_WORKERS"] = str(args.min_workers)

    from master.main import start_master

    start_master()


if __name__ == "__main__":
    main()
