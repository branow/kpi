import argparse
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, List, Tuple

from src.core.cluster.coordinator import Coordinator
from src.runtime.cluster_runtime import ClusterRuntime


def load_symbol(path: str) -> Callable[[], Any]:
    module_path, class_name = path.split(":", 1)
    mod = import_module(module_path)
    cls = getattr(mod, class_name)
    return cls


def cmd_run(args: argparse.Namespace) -> None:
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    cluster = ClusterRuntime(num_workers=args.workers, data_dir=input_dir)
    cluster.start()

    mapper_cls = load_symbol(args.job.split(",")[0])
    reducer_cls = load_symbol(args.job.split(",")[1])

    input_files = sorted(input_dir.glob("*.txt"))
    coord = Coordinator(
        bus=cluster.bus,
        worker_names=[f"worker-{i}" for i in range(args.workers)],
        num_reducers=args.reducers,
        mapper_factory=mapper_cls,
        reducer_factory=reducer_cls,
    )
    results = coord.run(input_files)

    out_path = output_dir / "part-00000.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        for k, v in sorted(results, key=lambda kv: str(kv[0])):
            f.write(f"{k}\t{v}\n")

    cluster.stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mapreduce")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run")
    run.add_argument("--workers", type=int, default=2)
    run.add_argument("--reducers", type=int, default=2)
    run.add_argument("--input", required=True)
    run.add_argument("--output", required=True)
    run.add_argument(
        "--job",
        required=True,
        help="<mapper_module:MapperClass>,<reducer_module:ReducerClass>",
    )
    run.set_defaults(func=cmd_run)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()


