import argparse
import os


def main():
    parser = argparse.ArgumentParser(
        description="Jalankan worker pemrosesan dokumen."
    )
    parser.add_argument(
        "--host",
        "--master-host",
        "--host-name",
        dest="host",
        default=os.environ.get("MASTER_ADDRESS", "127.0.0.1"),
        help="IP atau hostname komputer master (default: 127.0.0.1).",
    )
    args = parser.parse_args()

    os.environ["MASTER_ADDRESS"] = args.host

    from worker.client import start_worker

    start_worker()


if __name__ == "__main__":
    main()
