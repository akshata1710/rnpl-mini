"""Command line entry points.

  python -m app.cli init-db         drop + create tables + seed listings
  python -m app.cli worker [name]   one charge worker (start several!)
  python -m app.cli sweeper         rescue stuck plans, release overdue ones
  python -m app.cli relay           outbox -> broker -> consumers
  python -m app.cli all             2 workers + sweeper + relay in one process (easiest)
  python -m app.cli metrics         print backlog age + reconciliation
"""
import json
import logging
import sys
import threading

from app import db, metrics, relay, sweeper, worker


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-10s %(message)s", datefmt="%H:%M:%S")
    cmd = argv[0] if argv else "help"

    if cmd == "init-db":
        db.init_db()
        print("database ready")
    elif cmd == "worker":
        worker.run_forever(argv[1] if len(argv) > 1 else None)
    elif cmd == "sweeper":
        sweeper.run_forever()
    elif cmd == "relay":
        relay.run_forever()
    elif cmd == "all":
        stop = threading.Event()
        threads = [
            threading.Thread(target=worker.run_forever, args=("worker-a", stop), daemon=True),
            threading.Thread(target=worker.run_forever, args=("worker-b", stop), daemon=True),
            threading.Thread(target=sweeper.run_forever, args=(stop,), daemon=True),
            threading.Thread(target=relay.run_forever, args=(stop,), daemon=True),
        ]
        for t in threads:
            t.start()
        print("running 2 workers + sweeper + relay. Ctrl-C to stop.")
        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            stop.set()
    elif cmd == "metrics":
        with db.connect() as conn:
            print(json.dumps(metrics.snapshot(conn), indent=2, default=str))
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
