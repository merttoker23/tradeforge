import sys

from tradeforge.bootstrap import main

if __name__ == "__main__":
    if "--runtime" in sys.argv:
        from apps.quant_core.runtime import main as runtime_main

        sys.argv.remove("--runtime")
        raise SystemExit(runtime_main())
    if len(sys.argv) > 1 and sys.argv[1] == "replay":
        from tradeforge.research.replay import main as replay_main

        raise SystemExit(replay_main(sys.argv[2:]))
    raise SystemExit(main("quant_core", 8102))
