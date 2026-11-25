# Surge Hunter — Pump.fun Replay Sprint Paper Bot

[![make.fun](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/reonakisaragi4-bot/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/reonakisaragi4-bot/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

A deterministic paper trading bot for the [Pump.fun Replay Sprint — Paper Bot, Real Ranking](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122) bounty on [make.fun](https://make.fun). It trades 10 SOL of paper money on a 24h replay of pump.fun bonding-curve activity. Best net PnL wins.

## How it works

`main.ts` is a single self-contained TypeScript file (Node 22+, no dependencies).
It speaks the `agent-fun/replay/v2` protocol over stdin/stdout: it reads the
harness `hello`, folds each `batch` of events into its view of every curve, and
answers with buy/sell orders.

Strategy — **Surge Hunter**:

- **Entry:** a curve becomes a buy only after it has survived its launch dump
  (at least 20,000 slots since creation) and proves demand — several distinct
  buyers, net SOL flowing in, price up at least 40% over the window and
  accelerating (newer half of the window pulls in more than the older half),
  price near its window high. A composite heat score (price rise + net inflow
  + buyer breadth) sizes the first buy: hotter tape, bigger first buy. One buy
  per coin — no pyramiding, so a single bad call stays small.
- **Exits:** hard stop loss, trailing stop once the position is up, a first
  take-profit that banks half the position at +30% and lets the rest ride, a
  stall exit when the tape goes quiet, a big-sell exit on a single large dump,
  and a time cap.
- **Guardrails:** tiny filtered probe round trips keep the run above the
  20-coin minimum when the main signal goes quiet — a probe only buys a coin
  whose tape is already pulling in net SOL with a non-falling price; an equity
  brake stops new entries after a 20% drawdown from starting capital.

Deterministic by construction: no clock, no randomness, no network, no
environment reads. Every decision is a pure function of the event stream, so
two runs agree exactly.

## Run

```sh
node main.ts
```

## License

MIT — see [LICENSE](LICENSE).
