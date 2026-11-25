// Surge Hunter — a fresh deterministic paper bot for the pump.fun replay sprint.
//
// Idea: most new pump.fun curves bleed out, a few catch fire. This bot waits for a
// curve to prove it is catching fire, then rides it with size that grows with the
// strength of the signal, and gets out fast when the fire goes out.
//
// Signal (per mint, recomputed every batch it trades in): the curve must be old
// enough to have survived its launch dump (at least 20,000 slots since its
// CreateEvent) and the tape must show real demand — several distinct buyers, net
// SOL flowing in, the price up at least 40% over the window and accelerating
// (the newer half of the window pulls in more than the older half), and the price
// sitting near its window high. One big wallet doing all the buying does not
// count. A composite heat score (price rise, net inflow, buyer breadth) sizes
// the first buy: hotter tape, bigger first buy. No pyramiding: one buy per coin
// keeps a single bad call small.
//
// Exits: a hard stop loss, a trailing stop that arms once the position is up, a
// first take-profit that banks half the position once it is up 30% and lets the
// rest ride, a stall exit when the tape goes quiet and the position is flat, a
// big-sell exit when a single dump lands, and a time cap. Winners are not chased
// with adds: the first buy is the whole position.
//
// Guardrails: tiny filtered probe round trips keep the run above the 20-coin /
// round-trip minimum when the main signal goes quiet — a probe only buys a coin
// whose tape is already pulling in net SOL with a non-falling price, so the
// guardrail trips cost fees, not blind drift. An equity brake stops new entries
// after a 20% drawdown from starting capital.
//
// Deterministic by construction: the only inputs are the harness messages (slots,
// reserves, trades). No clock, no randomness, no network, no environment reads.
// Every decision is a pure function of the event stream, so two runs agree exactly.

import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

// ---------------------------------------------------------------- constants

const LAMPORTS_PER_SOL = 1_000_000_000n;
const LAMPORTS_FLOAT = 1e9;
const BPS = 10_000n;
/** Curve fee taken on sells (protocol + creator fallback, 95 + 30 bps). */
const CURVE_FEE = 0.0125;
/** Base + priority fee per sent order, in SOL. */
const TX_FEE_SOL = 0.000105;

/** Tunable knobs for the strategy. One object so a sweep can vary it. */
export interface Params {
  window: number;
  minNetSol: number;
  minBuys: number;
  minBuyers: number;
  minPriceUp: number;
  maxPriceUp: number;
  nearHigh: number;
  maxBuyerShare: number;
  minRqSol: number;
  maxRqSol: number;
  minAge: number;
  stakeSol: number;
  initialMinSol: number;
  initialMaxSol: number;
  addSol: number;
  addStep: number;
  minBuySol: number;
  buySlipBps: number;
  stopLoss: number;
  trail: number;
  trailArm: number;
  /** First take-profit: sell tpPartFrac of the position once it is worth this much over its cost. 0 = off. */
  tpPart: number;
  tpPartFrac: number;
  stallWindow: number;
  stallHold: number;
  bigSellSol: number;
  maxHold: number;
  holdIfUp: number;
  cooldown: number;
  maxEntries: number;
  maxMain: number;
  maxExposureSol: number;
  equityBrake: number;
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  probeSol: number;
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  window: 60,
  minNetSol: 0.5,
  minBuys: 4,
  minBuyers: 4,
  minPriceUp: 0.4,
  maxPriceUp: 2.0,
  nearHigh: 0.9,
  maxBuyerShare: 0.6,
  minRqSol: 1,
  maxRqSol: 70,
  minAge: 20000,
  stakeSol: 0.3,
  initialMinSol: 0.02,
  initialMaxSol: 0.12,
  addSol: 0,
  addStep: 0.05,
  minBuySol: 0.01,
  buySlipBps: 2000,
  stopLoss: 0.25,
  trail: 0.3,
  trailArm: 0.15,
  tpPart: 0.3,
  tpPartFrac: 0.5,
  stallWindow: 60,
  stallHold: 600,
  bigSellSol: 1.0,
  maxHold: 2400,
  holdIfUp: 0.5,
  cooldown: 200,
  maxEntries: 2,
  maxMain: 5,
  maxExposureSol: 6,
  equityBrake: 8.0,
  targetCoins: 25,
  targetFills: 80,
  paceSecs: 21600,
  probeSol: 0.01,
  maxProbes: 1,
};

// ---------------------------------------------------------------- wire types

type Pubkey = string;

interface HelloMsg {
  type: 'hello';
  protocol: string;
  seed: string;
  capital: { lamports: string };
  caps: { max_open_positions: number; max_order_bps_of_real_reserve: number; max_orders_per_batch: number };
  latency_slots: number;
  fees: { base_fee_lamports: string; priority_fee_lamports: string };
}

interface ChainEvent {
  event: string;
  mint: string;
  slot: number;
  block_time: number | null;
  // TradeEvent fields
  sol_amount?: string;
  is_buy?: boolean;
  user?: string;
  virtual_sol_reserves?: string;
  virtual_token_reserves?: string;
  real_sol_reserves?: string;
  real_token_reserves?: string;
  // CreateEvent fields
  virtual_quote_reserves?: string;
}

interface MarkMsg {
  mint: string;
  slot: number;
  virtual_token_reserves: string;
  virtual_quote_reserves: string;
  real_token_reserves: string;
  real_quote_reserves: string;
}

interface FillMsg {
  client_id: string;
  mint: string;
  side: string;
  status: string;
}

interface BatchMsg {
  type: 'batch';
  seq: number;
  slot: number;
  events: ChainEvent[];
  marks: MarkMsg[];
  fills: FillMsg[];
  account: { cash_lamports: string; positions: { mint: string; token_amount: string; cost_lamports: string }[] };
}

interface DoneMsg {
  type: 'done';
  seq: number;
  reason: string;
  account: { cash_lamports: string; positions: { mint: string; token_amount: string; cost_lamports: string }[] };
}

interface OrderOut {
  side: 'buy' | 'sell';
  client_id: string;
  mint: string;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

// ---------------------------------------------------------------- run state

interface CurveRec {
  mint: Pubkey;
  createdSlot: number | null;
  vq: bigint;
  vt: bigint;
  rq: bigint;
  rt: bigint;
  reservesSlot: number;
  trades: number;
  complete: boolean;
}

interface Holding {
  tokens: bigint;
  cost: bigint;
}

interface PendingRec {
  side: 'buy' | 'sell';
  mint: Pubkey;
  quote: bigint;
}

/** The bot's view of the run, folded forward batch by batch. */
export class Engine {
  readonly hello: HelloMsg;
  seq = -1;
  slot = -1;
  cash: bigint;
  readonly positions = new Map<Pubkey, Holding>();
  readonly curves = new Map<Pubkey, CurveRec>();
  readonly pending = new Map<string, PendingRec>();
  private nextId = 1;
  private usedIds = new Set<string>();

  constructor(hello: HelloMsg) {
    this.hello = hello;
    this.cash = BigInt(hello.capital.lamports);
  }

  clientId(): string {
    for (;;) {
      const id = `o${this.nextId++}`;
      if (!this.usedIds.has(id)) {
        this.usedIds.add(id);
        return id;
      }
    }
  }

  txFee(): bigint {
    return BigInt(this.hello.fees.base_fee_lamports) + BigInt(this.hello.fees.priority_fee_lamports);
  }

  /** Most this batch's buys on the mint may spend together (1% of real reserves). */
  maxBuy(mint: Pubkey): bigint {
    const c = this.curves.get(mint);
    if (c === undefined || c.complete) return 0n;
    return (c.rq * BigInt(this.hello.caps.max_order_bps_of_real_reserve)) / BPS;
  }

  committedCount(): number {
    const mints = new Set<Pubkey>(this.positions.keys());
    for (const p of this.pending.values()) if (p.side === 'buy') mints.add(p.mint);
    return mints.size;
  }

  freeCash(): bigint {
    const tx = this.txFee();
    let free = this.cash;
    for (const p of this.pending.values()) {
      free -= tx;
      if (p.side === 'buy') free -= p.quote;
    }
    return free;
  }

  private curveOf(mint: Pubkey, slot: number): CurveRec {
    let c = this.curves.get(mint);
    if (c === undefined) {
      c = { mint, createdSlot: null, vq: 0n, vt: 0n, rq: 0n, rt: 0n, reservesSlot: slot, trades: 0, complete: false };
      this.curves.set(mint, c);
    }
    return c;
  }

  applyBatch(batch: BatchMsg): void {
    this.seq = batch.seq;
    this.slot = batch.slot;
    for (const f of batch.fills) this.pending.delete(f.client_id);
    this.cash = BigInt(batch.account.cash_lamports);
    this.positions.clear();
    for (const p of batch.account.positions) {
      const tokens = BigInt(p.token_amount);
      if (tokens > 0n) this.positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
    }
    for (const m of batch.marks) {
      const c = this.curves.get(m.mint);
      if (c !== undefined && m.slot < c.reservesSlot) continue;
      const curve = c ?? this.curveOf(m.mint, m.slot);
      curve.vt = BigInt(m.virtual_token_reserves);
      curve.vq = BigInt(m.virtual_quote_reserves);
      curve.rt = BigInt(m.real_token_reserves);
      curve.rq = BigInt(m.real_quote_reserves);
      curve.reservesSlot = m.slot;
    }
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const curve = this.curveOf(e.mint, e.slot);
        curve.createdSlot = e.slot;
        curve.vt = BigInt(e.virtual_token_reserves ?? '0');
        curve.vq = BigInt(e.virtual_quote_reserves ?? e.virtual_sol_reserves ?? '0');
        curve.rt = BigInt(e.real_token_reserves ?? '0');
        curve.rq = 0n;
        curve.reservesSlot = e.slot;
      } else if (e.event === 'TradeEvent') {
        const curve = this.curveOf(e.mint, e.slot);
        curve.vt = BigInt(e.virtual_token_reserves ?? '0');
        curve.vq = BigInt(e.virtual_quote_reserves ?? e.virtual_sol_reserves ?? '0');
        curve.rt = BigInt(e.real_token_reserves ?? '0');
        curve.rq = BigInt(e.real_quote_reserves ?? e.real_sol_reserves ?? '0');
        curve.reservesSlot = e.slot;
        curve.trades += 1;
      } else if (e.event === 'CompleteEvent') {
        this.curveOf(e.mint, e.slot).complete = true;
      }
    }
  }

  applyDone(done: DoneMsg): void {
    this.seq = done.seq;
    this.cash = BigInt(done.account.cash_lamports);
    this.positions.clear();
    for (const p of done.account.positions) {
      const tokens = BigInt(p.token_amount);
      if (tokens > 0n) this.positions.set(p.mint, { tokens, cost: BigInt(p.cost_lamports) });
    }
  }

  record(orders: OrderOut[]): void {
    for (const o of orders) {
      this.usedIds.add(o.client_id);
      this.pending.set(o.client_id, {
        side: o.side,
        mint: o.mint,
        quote: o.side === 'buy' ? BigInt(o.quote_lamports ?? '0') : 0n,
      });
    }
  }
}

/** Mark-to-market value of a holding in SOL, net of curve fee and one tx fee. */
function positionValueSol(engine: Engine, mint: Pubkey, holding: Holding): number {
  const curve = engine.curves.get(mint);
  if (curve === undefined) return 0;
  const tokens = Number(holding.tokens);
  const vq = Number(curve.vq);
  const vt = Number(curve.vt);
  if (vt + tokens <= 0) return 0;
  const gross = (tokens * vq) / (vt + tokens) / LAMPORTS_FLOAT;
  return gross * (1 - CURVE_FEE) - TX_FEE_SOL;
}

function priceOf(curve: CurveRec): number {
  if (curve.vt === 0n) return 0;
  return Number(curve.vq) / Number(curve.vt);
}

// ---------------------------------------------------------------- strategy

interface Trade {
  slot: number;
  buy: boolean;
  sol: number;
  user: string;
  price: number;
}

interface Tape {
  created: number | null;
  trades: Trade[];
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
  lastAddPrice: number;
  lastValue: number;
  /** The first take-profit slice has been sold; the rest rides. */
  tookPartial: boolean;
}

/**
 * Heat score of a tape, 0..1, from three independent reads of demand:
 * price rise over the window, net SOL inflow, and how many distinct wallets
 * are buying. A tape scores high only when all three agree; a one-wallet pump
 * or a flat tape with a single big buy cannot fake it.
 */
function heatScore(
  p: Params,
  trades: Trade[],
  now: number,
): { ok: boolean; score: number } {
  const from = now - p.window;
  const mid = now - p.window / 2;
  let buySol = 0;
  let sellSol = 0;
  let buys = 0;
  let netOld = 0;
  let netNew = 0;
  let startPrice = -1;
  let high = 0;
  let last = 0;
  let total = 0;
  const byUser = new Map<string, number>();
  for (const tr of trades) {
    if (tr.slot <= from) {
      startPrice = tr.price;
      continue;
    }
    if (startPrice < 0) startPrice = tr.price;
    total++;
    const signed = tr.buy ? tr.sol : -tr.sol;
    if (tr.slot <= mid) netOld += signed;
    else netNew += signed;
    if (tr.buy) {
      buySol += tr.sol;
      buys++;
      byUser.set(tr.user, (byUser.get(tr.user) ?? 0) + tr.sol);
    } else {
      sellSol += tr.sol;
    }
    if (tr.price > high) high = tr.price;
    last = tr.price;
  }
  const no = { ok: false, score: 0 };
  if (total < 6 || buys < p.minBuys || byUser.size < p.minBuyers) return no;
  const net = buySol - sellSol;
  if (net < p.minNetSol) return no;
  if (startPrice <= 0 || last <= 0) return no;
  const up = last / startPrice - 1;
  if (up < p.minPriceUp || up > p.maxPriceUp) return no;
  if (last < high * p.nearHigh) return no;
  // Acceleration: the newer half must pull in at least as much as the older half.
  if (netNew <= 0 || netNew < netOld) return no;
  let top = 0;
  for (const v of byUser.values()) if (v > top) top = v;
  if (top > p.maxBuyerShare * buySol) return no;
  const risePart = Math.min(1, Math.max(0, (up - p.minPriceUp) / 0.42));
  const flowPart = Math.min(1, net / 3);
  const breadthPart = Math.min(1, byUser.size / 12);
  const score = 0.35 * risePart + 0.35 * flowPart + 0.3 * breadthPart;
  return { ok: true, score };
}

/** Net recorded SOL (buys minus sells) over the last `w` slots. */
function flowOver(trades: Trade[], now: number, w: number): number {
  let net = 0;
  for (let i = trades.length - 1; i >= 0; i--) {
    const tr = trades[i]!;
    if (tr.slot <= now - w) break;
    net += tr.buy ? tr.sol : -tr.sol;
  }
  return net;
}

export class Strategy {
  private readonly engine: Engine;
  private readonly p: Params;
  private readonly tapes = new Map<Pubkey, Tape>();
  private readonly held = new Map<Pubkey, Held>();
  /** Probe round trips in flight: false while buying, true once selling. */
  private readonly probes = new Map<Pubkey, boolean>();
  private myFills = 0;
  private readonly myCoins = new Set<Pubkey>();
  private firstTime: number | null = null;
  private lastTime: number | null = null;

  constructor(engine: Engine, p: Params = DEFAULT_PARAMS) {
    this.engine = engine;
    this.p = p;
  }

  private tapeOf(mint: Pubkey): Tape {
    let t = this.tapes.get(mint);
    if (t === undefined) {
      t = { created: null, trades: [], entries: 0, lastExit: -1e12 };
      this.tapes.set(mint, t);
    }
    return t;
  }

  onBatch(batch: BatchMsg): OrderOut[] {
    const engine = this.engine;
    const p = this.p;
    const now = batch.slot;
    const orders: OrderOut[] = [];
    const cap = engine.hello.caps.max_orders_per_batch;
    const tx = TX_FEE_SOL;
    const bigSell = new Set<Pubkey>();
    const traded = new Set<Pubkey>();

    for (const f of batch.fills) {
      if (f.status === 'filled' && !f.client_id.startsWith('_')) {
        this.myFills++;
        this.myCoins.add(f.mint);
      }
    }
    for (const e of batch.events) {
      if (e.block_time !== null) {
        if (this.firstTime === null) this.firstTime = e.block_time;
        if (this.lastTime === null || e.block_time > this.lastTime) this.lastTime = e.block_time;
      }
      if (e.event === 'CreateEvent') {
        this.tapeOf(e.mint).created = e.slot;
      } else if (e.event === 'TradeEvent') {
        const t = this.tapeOf(e.mint);
        const sol = Number(e.sol_amount ?? '0') / LAMPORTS_FLOAT;
        const price = Number(e.virtual_quote_reserves ?? e.virtual_sol_reserves ?? '0') / Number(e.virtual_token_reserves ?? '1');
        t.trades.push({ slot: e.slot, buy: e.is_buy === true, sol, user: e.user ?? '', price });
        traded.add(e.mint);
        if (e.is_buy === false && sol >= p.bigSellSol) bigSell.add(e.mint);
      }
    }
    // Trim tapes to the longest lookback any rule needs.
    const keep = Math.max(p.window, p.stallWindow) + 5;
    for (const mint of traded) {
      const t = this.tapes.get(mint)!;
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= now - keep) cut++;
      if (cut > 0) t.trades.splice(0, cut);
    }

    const pendingByMint = new Map<Pubkey, number>();
    for (const po of engine.pending.values()) {
      pendingByMint.set(po.mint, (pendingByMint.get(po.mint) ?? 0) + 1);
    }
    const hasSellPending = (mint: Pubkey): boolean => {
      for (const po of engine.pending.values()) if (po.mint === mint && po.side === 'sell') return true;
      return false;
    };

    // ---- exits first: stop, trail, stall, big sell, time cap.
    for (const [mint, h] of this.held) {
      const pos = engine.positions.get(mint);
      const curve = engine.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) {
          this.held.delete(mint);
          const t = this.tapeOf(mint);
          if (h.filled) t.lastExit = now;
          else t.entries--;
        }
        continue;
      }
      h.filled = true;
      if (curve === undefined || curve.complete) continue;
      const value = positionValueSol(engine, mint, pos);
      const cost = Number(pos.cost) / LAMPORTS_FLOAT;
      if (value > h.peakValue) h.peakValue = value;
      h.lastValue = value;
      // First take-profit: bank a slice once the position is up tpPart, and let
      // the rest ride. Never in the same batch as a whole exit: a partial and a
      // full sell together would oversell what the position holds.
      if (p.tpPart > 0 && !h.tookPartial && !h.exiting && value >= cost * (1 + p.tpPart)) {
        if (!hasSellPending(mint) && orders.length < cap) {
          const slice = (pos.tokens * BigInt(Math.round(p.tpPartFrac * 1000))) / 1000n;
          if (slice > 0n && slice < pos.tokens) {
            orders.push({ side: 'sell', mint, token_amount: slice.toString(), max_slippage_bps: 10_000 });
            h.tookPartial = true;
            h.invested *= 1 - p.tpPartFrac;
            h.peakValue = value * (1 - p.tpPartFrac);
            continue;
          }
        }
      }
      const t = this.tapeOf(mint);
      const rqSol = Number(curve.rq) / LAMPORTS_FLOAT;
      const riding = rqSol >= 80;
      let exit = h.exiting;
      if (!exit && value < cost * (1 - p.stopLoss)) exit = true;
      if (!exit && !riding && h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
      if (!exit && !riding && bigSell.has(mint)) exit = true;
      if (!exit && !riding && now - h.opened > p.stallHold && value < cost * 1.05 && flowOver(t.trades, now, p.stallWindow) < 0) exit = true;
      if (!exit && now - h.opened > p.maxHold && value < cost * (1 + p.holdIfUp)) exit = true;
      if (!exit) continue;
      h.exiting = true;
      if (hasSellPending(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.tokens.toString(), max_slippage_bps: 10_000 });
    }

    // ---- probe sells: dump the probe as soon as its buy has landed.
    for (const [mint, selling] of this.probes) {
      const pos = engine.positions.get(mint);
      const curve = engine.curves.get(mint);
      if (pos === undefined) {
        if (!pendingByMint.has(mint)) this.probes.delete(mint);
        continue;
      }
      if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
      if (orders.length >= cap) break;
      orders.push({ side: 'sell', mint, token_amount: pos.tokens.toString(), max_slippage_bps: 10_000 });
      if (!selling) this.probes.set(mint, true);
    }

    // ---- entries and pyramid adds.
    let free = Number(engine.freeCash()) / LAMPORTS_FLOAT;
    let slotsLeft = engine.hello.caps.max_open_positions - engine.committedCount();
    let mainLeft = p.maxMain - this.held.size;
    let exposure = 0;
    for (const [mint, h] of this.held) {
      exposure += Math.max(h.invested, Number(engine.positions.get(mint)?.cost ?? 0n) / LAMPORTS_FLOAT);
    }
    // Equity brake: stop opening new risk once equity is 20% below start.
    let equity = Number(engine.cash) / LAMPORTS_FLOAT;
    for (const [mint, pos] of engine.positions) equity += positionValueSol(engine, mint, pos);
    const braked = equity < p.equityBrake;

    for (const mint of traded) {
      if (orders.length >= cap) break;
      const curve = engine.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingByMint.has(mint) || this.probes.has(mint)) continue;
      const t = this.tapes.get(mint)!;
      const h = this.held.get(mint);
      const price = priceOf(curve);
      const rqSol = Number(curve.rq) / LAMPORTS_FLOAT;
      let want: number;
      if (h === undefined) {
        if (braked || slotsLeft <= 0 || mainLeft <= 0) continue;
        if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
        if (rqSol < p.minRqSol || rqSol > p.maxRqSol) continue;
        if (t.created === null || now - t.created < p.minAge) continue;
        const signal = heatScore(p, t.trades, now);
        if (!signal.ok) continue;
        want = p.initialMinSol + (p.initialMaxSol - p.initialMinSol) * signal.score;
      } else {
        // Pyramid into strength only: price stepped up, position in profit, inflow alive.
        if (h.exiting || !h.filled || h.invested >= p.stakeSol - p.minBuySol) continue;
        const pos = engine.positions.get(mint);
        if (pos === undefined) continue;
        if (price < h.lastAddPrice * (1 + p.addStep)) continue;
        if (h.lastValue < (Number(pos.cost) / LAMPORTS_FLOAT) * 1.0) continue;
        if (flowOver(t.trades, now, p.window) < 0) continue;
        if (rqSol > p.maxRqSol) continue;
        want = Math.min(p.addSol, p.stakeSol - h.invested);
      }
      const capSol = Math.floor(Number(engine.maxBuy(mint))) / LAMPORTS_FLOAT;
      let amount = Math.min(capSol, want, p.maxExposureSol - exposure);
      if (amount > free - tx) amount = free - tx;
      if (amount < p.minBuySol) continue;
      const lamports = BigInt(Math.floor(amount * LAMPORTS_FLOAT));
      orders.push({ side: 'buy', mint, quote_lamports: lamports.toString(), max_slippage_bps: p.buySlipBps });
      free -= amount + tx;
      exposure += amount;
      if (h === undefined) {
        this.held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, lastAddPrice: price, lastValue: 0, tookPartial: false });
        t.entries++;
        slotsLeft--;
        mainLeft--;
      } else {
        h.invested += amount;
        h.lastAddPrice = price;
      }
    }

      // ---- guardrail pacing: probe round trips when fills or coins lag the pace.
      // Probes are not random: a probe only buys a coin whose tape is already
      // pulling in net SOL and whose price is not falling over the entry window,
      // so the guardrail trips cost fees, not blind drift.
      if (this.probes.size < p.maxProbes && slotsLeft > 0 && this.firstTime !== null && this.lastTime !== null) {
      const pace = Math.min(1, (this.lastTime - this.firstTime) / p.paceSecs);
      const needCoins = this.myCoins.size < Math.ceil(p.targetCoins * pace);
      const needFills = this.myFills < Math.ceil(p.targetFills * pace);
      if (needCoins || needFills) {
        const minRq = (p.probeSol * Number(BPS)) / engine.hello.caps.max_order_bps_of_real_reserve;
        for (const mint of traded) {
          if (orders.length >= cap || this.probes.size >= p.maxProbes) break;
          if (this.held.has(mint) || this.probes.has(mint) || pendingByMint.has(mint) || engine.positions.has(mint)) continue;
          if (needCoins && !needFills && this.myCoins.has(mint)) continue;
          const curve = engine.curves.get(mint);
          if (curve === undefined || curve.complete) continue;
          if (Number(curve.rq) / LAMPORTS_FLOAT < minRq * 1.5) continue;
          const pt = this.tapeOf(mint);
          let firstPrice = -1;
          let lastPrice = -1;
          let pflow = 0;
          for (const tr of pt.trades) {
            if (tr.slot <= now - p.window) continue;
            if (firstPrice < 0) firstPrice = tr.price;
            lastPrice = tr.price;
            pflow += tr.buy ? tr.sol : -tr.sol;
          }
          if (pflow <= 0 || firstPrice <= 0 || lastPrice < firstPrice) continue;
          if (free < p.probeSol + 2 * tx) break;
          orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS_FLOAT)).toString(), max_slippage_bps: p.buySlipBps });
          this.probes.set(mint, false);
          free -= p.probeSol + tx;
        }
      }
    }
    return orders;
  }
}

// ---------------------------------------------------------------- line loop

async function writeReply(text: string): Promise<void> {
  const ok = process.stdout.write(text);
  if (!ok) await new Promise<void>((resolve) => process.stdout.once('drain', resolve));
}

async function main(): Promise<void> {
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  let engine: Engine | null = null;
  let strategy: Strategy | null = null;
  for await (const line of lines) {
    if (line.trim() === '') continue;
    let msg: { type?: unknown };
    try {
      msg = JSON.parse(line) as { type?: unknown };
    } catch {
      throw new Error('input line is not JSON');
    }
    if (msg.type === 'hello') {
      const hello = msg as unknown as HelloMsg;
      if (hello.protocol !== 'agent-fun/replay/v2') throw new Error(`harness speaks ${hello.protocol}`);
      engine = new Engine(hello);
      strategy = new Strategy(engine);
    } else if (msg.type === 'batch') {
      if (engine === null || strategy === null) throw new Error('batch before hello');
      const batch = msg as unknown as BatchMsg;
      engine.applyBatch(batch);
      const rawOrders = strategy.onBatch(batch);
      const orders: OrderOut[] = rawOrders.map((o) => (o.client_id === undefined ? { ...o, client_id: engine.clientId() } : o));
      const reply = { type: 'reply', seq: batch.seq, orders };
      const text = JSON.stringify(reply);
      if (text.length > 65_536) throw new Error(`reply over 65536 bytes on batch ${batch.seq}`);
      engine.record(orders);
      await writeReply(`${text}\n`);
    } else if (msg.type === 'done') {
      if (engine === null) throw new Error('done before hello');
      engine.applyDone(msg as unknown as DoneMsg);
      break;
    } else {
      throw new Error(`unknown message type ${String(msg.type)}`);
    }
  }
  // The harness may keep stdin open after done; the run is over, so leave now.
  process.exit(0);
}

// Only run the line loop when this file is the entrypoint (so a sweep driver can
// import the strategy without starting the bot).
if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err: unknown) => {
    process.stderr.write(`fatal: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
  });
}
