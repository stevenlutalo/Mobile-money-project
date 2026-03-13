"""
tools/multi_user_sim.py — Simulate many users against the cluster from ONE PC.

Each virtual user is an independent thread with its own JWT token, account,
and server-discovery instance — exactly as if it were running on a separate PC.

Usage:
  python tools/multi_user_sim.py
  python tools/multi_user_sim.py --users 20 --ops 10
  python tools/multi_user_sim.py --users 50 --ops 20 --seed-money 100000

Options:
  --users       Number of virtual users to spawn     (default: 10)
  --ops         Operations per user                  (default: 8)
  --seed-money  Starting UGX deposited on login      (default: 50000)
  --think-time  Seconds between operations per user  (default: 0.2)

Operations each user runs (random mix):
  • create_account  — once, at startup
  • deposit         — adds UGX to own account
  • get_balance     — reads own balance
  • transfer        — sends UGX to another virtual user's account
"""

# ── Standard library ──────────────────────────────────────────────────────────
import argparse
import concurrent.futures
import json
import logging
import os
import random
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

# ── Allow running from the project root ──────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Suppress all INFO/DEBUG noise so the live dashboard stays clean.
logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)

# ── Project imports ───────────────────────────────────────────────────────────
try:
    from core.consistent_ring import NodeRing
    from core.exceptions import NodeUnreachableError
    from services.auth_service import AuthService
    from client.client import NearestServerDiscovery
except ImportError as exc:
    print(
        f"\n✗ Import error: {exc}"
        "\n  Run this from the project root:"
        "\n    python tools/multi_user_sim.py\n"
    )
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

SECRET_KEY   = "your-shared-secret-key-min-32-chars!"
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", "mmoney-cluster")
ETCD_HOST    = os.environ.get("ETCD_HOST",    "localhost")
ETCD_PORT    = int(os.environ.get("ETCD_PORT", 2379))

# Amount range for random deposits (UGX)
DEPOSIT_CHOICES  = [1_000, 2_000, 5_000, 10_000]
# Amount range for random transfers (UGX)
TRANSFER_CHOICES = [500, 1_000, 2_000]

# Weighted operation menu: deposit and balance are more common than transfer
OP_WEIGHTS = {
    "deposit":  4,
    "balance":  3,
    "transfer": 3,
}

# ─────────────────────────────────────────────────────────────────────────────
# PER-USER STATS  (updated by worker threads, read by dashboard thread)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class UserStats:
    index:       int
    user_id:     str
    account_id:  str
    ops_done:    int   = 0
    ops_ok:      int   = 0
    ops_fail:    int   = 0
    total_lat_ms: float = 0.0
    last_op:     str   = "—"
    last_err:    str   = ""
    final_balance: float = 0.0
    done:        bool  = False


_stats_lock: threading.Lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# RPC HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _rpc(discovery: NearestServerDiscovery, fn) -> tuple:
    """Call fn(conn) via discovery.  Returns (result, latency_ms)."""
    t0     = time.perf_counter()
    result = discovery.execute_with_fallback(fn)
    lat    = (time.perf_counter() - t0) * 1000
    return result, lat


# ─────────────────────────────────────────────────────────────────────────────
# VIRTUAL USER WORKER  (one thread per user)
# ─────────────────────────────────────────────────────────────────────────────

def user_worker(
    stats:        UserStats,
    token:        str,
    discovery:    NearestServerDiscovery,
    all_accounts: List[str],
    n_ops:        int,
    seed_money:   float,
    think_time:   float,
) -> None:
    """
    Simulates one user's full session:
      1. Create account with seed_money
      2. n_ops random operations (deposit / balance / transfer)
      3. Final balance check
    """
    acct = stats.account_id

    # ── 1. Create account ────────────────────────────────────────────────
    try:
        _, lat = _rpc(
            discovery,
            lambda conn, a=acct, s=seed_money:
                conn.root.exposed_create_account(token, a, s),
        )
        with _stats_lock:
            stats.ops_done  += 1
            stats.ops_ok    += 1
            stats.total_lat_ms += lat
            stats.last_op   = f"create  seed={seed_money:,.0f}"
    except Exception as exc:
        with _stats_lock:
            stats.ops_done += 1
            stats.ops_fail += 1
            stats.last_op  = "create"
            stats.last_err = str(exc)[:55]

    time.sleep(think_time)

    # ── 2. Main operation loop ────────────────────────────────────────────
    op_pool = [op for op, w in OP_WEIGHTS.items() for _ in range(w)]

    for _ in range(n_ops):
        op = random.choice(op_pool)

        try:
            if op == "deposit":
                amount = random.choice(DEPOSIT_CHOICES)
                _, lat = _rpc(
                    discovery,
                    lambda conn, a=acct, amt=amount:
                        conn.root.exposed_deposit(token, a, amt),
                )
                label = f"deposit  +{amount:,} UGX"

            elif op == "balance":
                result, lat = _rpc(
                    discovery,
                    lambda conn, a=acct:
                        conn.root.exposed_get_balance(token, a),
                )
                label = f"balance  → {result:,.0f} UGX"

            else:  # transfer
                others = [a for a in all_accounts if a != acct]
                if not others:
                    # Only one user; degrade to balance check
                    result, lat = _rpc(
                        discovery,
                        lambda conn, a=acct:
                            conn.root.exposed_get_balance(token, a),
                    )
                    label = f"balance  → {result:,.0f} UGX"
                else:
                    to_acct = random.choice(others)
                    amount  = random.choice(TRANSFER_CHOICES)
                    _, lat = _rpc(
                        discovery,
                        lambda conn, fa=acct, ta=to_acct, amt=amount:
                            conn.root.exposed_transfer(token, fa, ta, amt),
                    )
                    label = f"transfer −{amount:,} → {to_acct}"

            with _stats_lock:
                stats.ops_done     += 1
                stats.ops_ok       += 1
                stats.total_lat_ms += lat
                stats.last_op      = label

        except Exception as exc:
            with _stats_lock:
                stats.ops_done += 1
                stats.ops_fail += 1
                stats.last_op  = op
                stats.last_err = str(exc)[:55]

        time.sleep(think_time)

    # ── 3. Final balance ──────────────────────────────────────────────────
    try:
        balance, lat = _rpc(
            discovery,
            lambda conn, a=acct:
                conn.root.exposed_get_balance(token, a),
        )
        with _stats_lock:
            stats.final_balance = float(balance)
            stats.ops_done      += 1
            stats.ops_ok        += 1
            stats.total_lat_ms  += lat
            stats.last_op       = f"done  bal={balance:,.0f} UGX"
    except Exception as exc:
        with _stats_lock:
            stats.last_err = str(exc)[:55]

    with _stats_lock:
        stats.done = True


# ─────────────────────────────────────────────────────────────────────────────
# LIVE DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

_ANSI_HOME  = "\033[H"
_ANSI_CLEAR = "\033[2J"
_COL        = 80


def _dashboard(all_stats: List[UserStats], elapsed: float, final: bool = False) -> None:
    """Print (or redraw) the per-user progress table."""
    if not final:
        sys.stdout.write(_ANSI_HOME)

    mode = "FINAL REPORT" if final else f"LIVE — {elapsed:5.1f}s elapsed"
    print(f"\n  mmoney Multi-User Simulator  [{mode}]")
    print("  " + "─" * _COL)
    print(
        f"  {'User':<12}  {'Account':<10}  "
        f"{'Done':>5}  {'OK':>5}  {'Fail':>4}  "
        f"{'Avg ms':>8}  {'Last operation'}"
    )
    print("  " + "─" * _COL)

    for s in all_stats:
        avg = s.total_lat_ms / s.ops_done if s.ops_done else 0.0
        tick = "✓" if s.done else "·"
        print(
            f"  {s.user_id:<12}  {s.account_id:<10}  "
            f"{s.ops_done:>5}  {s.ops_ok:>5}  {s.ops_fail:>4}  "
            f"{avg:>7.1f}ms  {s.last_op:<40} {tick}"
        )

    print("  " + "─" * _COL)

    # Totals row
    total_done = sum(s.ops_done     for s in all_stats) or 1
    total_ok   = sum(s.ops_ok       for s in all_stats)
    total_fail = sum(s.ops_fail     for s in all_stats)
    total_lat  = sum(s.total_lat_ms for s in all_stats)
    avg_all    = total_lat / total_done
    users_done = sum(1 for s in all_stats if s.done)

    print(
        f"  {'TOTAL':<12}  {'':<10}  "
        f"{total_done:>5}  {total_ok:>5}  {total_fail:>4}  "
        f"{avg_all:>7.1f}ms"
    )
    print(f"  Users finished: {users_done} / {len(all_stats)}")

    if final:
        print("\n  Final balances:")
        for s in all_stats:
            err_note = f"  ← last error: {s.last_err}" if s.last_err else ""
            print(
                f"    {s.account_id:<12}  UGX {s.final_balance:>14,.2f}{err_note}"
            )

    sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY HELPER  (run in parallel for all users)
# ─────────────────────────────────────────────────────────────────────────────

def _build_discovery(ring: NodeRing, idx: int) -> NearestServerDiscovery:
    """Create and run discovery for one virtual user."""
    d = NearestServerDiscovery(ring)
    d.run_discovery()
    return d


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate multiple concurrent users against the mmoney cluster.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--users",      type=int,   default=10,    help="Number of virtual users")
    parser.add_argument("--ops",        type=int,   default=8,     help="Operations per user (after account creation)")
    parser.add_argument("--seed-money", type=float, default=50_000, help="Starting UGX per user account")
    parser.add_argument("--think-time", type=float, default=0.2,   help="Seconds between operations per user")
    args = parser.parse_args()

    n_users    = args.users
    n_ops      = args.ops
    seed_money = args.seed_money
    think_time = args.think_time

    # ── Banner ────────────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  mmoney — Multi-User Simulator")
    print("═" * 60)
    print(f"  Virtual users  : {n_users}")
    print(f"  Ops / user     : {n_ops}  (plus create + final balance)")
    print(f"  Seed money     : UGX {seed_money:,.0f} per account")
    print(f"  Think time     : {think_time}s between operations")
    print(f"  Cluster        : {ETCD_HOST}:{ETCD_PORT}  [{CLUSTER_NAME}]")
    print("═" * 60 + "\n")

    # ── Connect ring ──────────────────────────────────────────────────────
    print("Step 1/3  Connecting to etcd cluster registry...")
    try:
        ring = NodeRing(ETCD_HOST, ETCD_PORT, CLUSTER_NAME)
    except Exception as exc:
        print(f"  ✗ Could not reach etcd at {ETCD_HOST}:{ETCD_PORT}: {exc}")
        print("  Hint: make sure 'docker compose up' is running first.\n")
        sys.exit(1)
    print("  ✓ Cluster registry reachable\n")

    # ── Pre-assign account IDs ────────────────────────────────────────────
    # e.g. VSIM001, VSIM002, … so cross-transfers work from the start.
    account_ids = [f"VSIM{i+1:03d}" for i in range(n_users)]

    # ── Spin up auth tokens ───────────────────────────────────────────────
    auth   = AuthService(SECRET_KEY)
    tokens = [
        auth.issue_token(subject=f"vsim_user_{i+1:03d}", role="user")
        for i in range(n_users)
    ]

    # ── Parallel discovery  (each user finds its own best server) ─────────
    print(f"Step 2/3  Running server discovery for {n_users} virtual users in parallel...")
    discoveries: List[NearestServerDiscovery] = [None] * n_users
    failed_idx: List[int] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(n_users, 20)) as pool:
        future_to_idx = {
            pool.submit(_build_discovery, ring, i): i
            for i in range(n_users)
        }
        done_count = 0
        for fut in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                discoveries[idx] = fut.result()
                done_count += 1
                # Simple inline progress so the terminal isn't blank
                sys.stdout.write(f"\r  Discovered: {done_count}/{n_users}  ")
                sys.stdout.flush()
            except Exception as exc:
                failed_idx.append(idx)
                print(f"\n  ✗ Discovery failed for user {idx+1}: {exc}")

    print()
    if failed_idx:
        print(f"\n  ✗ {len(failed_idx)} user(s) failed discovery – aborting.\n")
        sys.exit(1)

    primary = discoveries[0].get_primary()
    print(
        f"  ✓ All {n_users} users connected  "
        f"(primary: {primary['node_id']}  "
        f"RTT: {primary.get('tcp_rtt_ms', 0):.1f}ms)\n"
    )

    # ── Build stats objects ───────────────────────────────────────────────
    all_stats: List[UserStats] = [
        UserStats(
            index      = i,
            user_id    = f"user_{i+1:03d}",
            account_id = account_ids[i],
        )
        for i in range(n_users)
    ]

    # ── Spawn worker threads ──────────────────────────────────────────────
    print(f"Step 3/3  Launching {n_users} concurrent users...\n")
    sys.stdout.write(_ANSI_CLEAR)   # clear screen once before dashboard takes over

    threads = [
        threading.Thread(
            target=user_worker,
            args=(
                all_stats[i],
                tokens[i],
                discoveries[i],
                account_ids,
                n_ops,
                seed_money,
                think_time,
            ),
            daemon=True,
        )
        for i in range(n_users)
    ]

    t_start = time.perf_counter()
    for t in threads:
        t.start()

    # ── Live dashboard loop ───────────────────────────────────────────────
    try:
        while any(t.is_alive() for t in threads):
            elapsed = time.perf_counter() - t_start
            with _stats_lock:
                snapshot = list(all_stats)
            _dashboard(snapshot, elapsed, final=False)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\n  Interrupted — waiting for threads to finish...\n")

    for t in threads:
        t.join(timeout=60)

    # ── Final report ──────────────────────────────────────────────────────
    elapsed = time.perf_counter() - t_start
    _dashboard(all_stats, elapsed, final=True)

    total_ops  = sum(s.ops_done for s in all_stats)
    total_ok   = sum(s.ops_ok   for s in all_stats)
    total_fail = sum(s.ops_fail for s in all_stats)
    rate       = 100 * total_ok / total_ops if total_ops else 0
    throughput = total_ops / elapsed if elapsed > 0 else 0

    print(f"\n{'═'*60}")
    print(f"  SUMMARY")
    print(f"{'═'*60}")
    print(f"  Total operations : {total_ops}")
    print(f"  Succeeded        : {total_ok}  ({rate:.1f}%)")
    print(f"  Failed           : {total_fail}")
    print(f"  Wall-clock time  : {elapsed:.1f}s")
    print(f"  Throughput       : {throughput:.1f} ops / sec")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
