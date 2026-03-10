===============================================================================
  DISTRIBUTED MOBILE MONEY SYSTEM — SETUP GUIDE
  Python + rpyc RPC | Dijkstra Auto-Routing | 3 Nodes | Console Only
===============================================================================

FILES IN THIS PROJECT
---------------------
  config.py        Node IP addresses and THIS_NODE setting
  accounts.json    Local account database (JSON)
  node_server.py   RPC server — run this on EACH PC
  sync_service.py  Replication module (called automatically by node_server)
  client.py        Interactive console client with Dijkstra auto-routing
  transactions.log Auto-created log of every transaction (one per node)


HOW THE CLIENT FINDS THE NEAREST NODE (Dijkstra's Algorithm)
--------------------------------------------------------------
The client NEVER asks you to choose a location. Instead, at startup it
automatically selects the best node using the following pipeline:

  STEP 1 — Network Scan
    The client probes every node in config.py via rpyc, sends 3 pings each,
    and records the minimum round-trip time (ms) to each node.

  STEP 2 — Topology Discovery
    For every reachable node, the client calls get_neighbour_latencies()
    over RPC. Each node returns its own measured latency to all other nodes.

  STEP 3 — Build Weighted Graph
    A graph is constructed with "CLIENT" and all node keys as vertices.
    Edge weights = measured latencies in milliseconds.
    Example graph:
        CLIENT  -> ug.hoi    8.42 ms
        CLIENT  -> ug.mba   12.17 ms
        CLIENT  -> ug.kam    UNREACHABLE (no direct edge added)
        ug.hoi  -> ug.mba    5.83 ms
        ug.hoi  -> ug.kam    4.11 ms
        ug.mba  -> ug.hoi    5.91 ms
        ug.mba  -> ug.kam    3.95 ms

  STEP 4 — Dijkstra's Algorithm
    Dijkstra runs from "CLIENT" using a min-heap priority queue.
    It finds the shortest (lowest latency) path to every node.
    Even if CLIENT cannot reach ug.kam directly, Dijkstra finds the path
    CLIENT -> ug.hoi -> ug.kam  (8.42 + 4.11 = 12.53 ms total).

  STEP 5 — Decision
    The node with the lowest total Dijkstra distance from CLIENT is selected.
    The client connects to it automatically and prints the result.

Console output example:

  [NETWORK SCAN] Probing all nodes from this client...
  -------------------------------------------------------
    Probing ug.hoi (Hoima) at 192.168.1.10:18861 ...    8.420 ms
    Probing ug.mba (Mbarara) at 192.168.1.20:18861 ...  12.170 ms
    Probing ug.kam (Kampala) at 192.168.1.30:18861 ...  UNREACHABLE
  -------------------------------------------------------

  [TOPOLOGY]  Fetching inter-node latency table from each reachable node...
  -------------------------------------------------------
    ug.hoi neighbours: ug.mba=5.830ms,  ug.kam=4.110ms
    ug.mba neighbours: ug.hoi=5.910ms,  ug.kam=3.950ms
  -------------------------------------------------------

  [DIJKSTRA]  Weighted graph edges:
  -------------------------------------------------------
    CLIENT       ->  ug.hoi        8.420 ms
    CLIENT       ->  ug.mba       12.170 ms
    ug.hoi       ->  ug.kam        4.110 ms
    ug.hoi       ->  ug.mba        5.830 ms
    ug.mba       ->  ug.kam        3.950 ms
    ug.mba       ->  ug.hoi        5.910 ms
  -------------------------------------------------------

  [DIJKSTRA]  Shortest paths from CLIENT:
  -------------------------------------------------------
    Node           Total Latency  Path
  -------------------------------------------------------
    ug.hoi              8.420 ms  CLIENT -> ug.hoi
    ug.mba             12.170 ms  CLIENT -> ug.mba
    ug.kam             12.530 ms  CLIENT -> ug.hoi -> ug.kam
  -------------------------------------------------------

  [DECISION]  Nearest node: ug.hoi  (8.420 ms total latency)
              Connecting to ug.hoi (192.168.1.10:18861)...  Connected.


STEP 0 — PREREQUISITES
-----------------------
All three PCs must be on the same WiFi network or connected by LAN.

Install the required library on ALL PCs:

    pip install rpyc

Python 3.10 or higher is required (uses X | Y union type hints).


STEP 1 — FIND YOUR IP ADDRESSES
--------------------------------
On EACH Windows PC, open Command Prompt and run:

    ipconfig

Look for "IPv4 Address" under your active adapter.

Examples:
  PC 1 (ug.hoi)  IPv4: 192.168.1.10
  PC 2 (ug.mba)  IPv4: 192.168.1.20
  PC 3 (ug.kam)  IPv4: 192.168.1.30


STEP 2 — COPY FILES TO ALL THREE PCs
--------------------------------------
Copy the entire MobileMoney folder to ALL THREE computers.
Every PC needs the same set of files.


STEP 3 — EDIT config.py ON EACH PC
--------------------------------------
Open config.py in Notepad and make two changes:

  A) Update the IP addresses to match your real network:

        NODE_A = { "host": "192.168.1.10", ... }   <-- PC 1's actual IPv4
        NODE_B = { "host": "192.168.1.20", ... }   <-- PC 2's actual IPv4
        NODE_C = { "host": "192.168.1.30", ... }   <-- PC 3's actual IPv4

     These three lines must be IDENTICAL on all three PCs.

  B) Set THIS_NODE to identify which node this machine is:

        On PC 1:   THIS_NODE = "ug.hoi"
        On PC 2:   THIS_NODE = "ug.mba"
        On PC 3:   THIS_NODE = "ug.kam"

  The client ignores THIS_NODE — only the servers use it.


STEP 4 — START THE SERVERS
----------------------------
On each of the three PCs, open Command Prompt in the MobileMoney folder:

    python node_server.py

Expected output on PC 1:
    =======================================================
      DISTRIBUTED MOBILE MONEY — RPC SERVER
    =======================================================
      Node ID   : ug.hoi
      Node Name : Node ug.hoi
      Location  : Hoima
      Listening : 0.0.0.0:18861
    =======================================================
      Server running. Press Ctrl+C to stop.

All three servers should be running before you start the client.


STEP 5 — RUN THE CLIENT (on any PC)
-------------------------------------
Open another Command Prompt window on any PC and run:

    python client.py

The client will perform the Dijkstra scan automatically (see above),
then prompt only for your Account ID.

    Enter Account ID: ACC001
    Welcome, Alice Nakato!
    Current balance: UGX 100,000
    Connected via  : Node A (Hoima)

    Choose operation:
      1. Deposit
      2. Withdraw
      3. Check Balance
      4. List All Accounts
      5. Exit

    > 1
    Enter amount to deposit: UGX 50000

    [Node A] Processing deposit of UGX 50,000 to ACC001...
    -------------------------------------------------------
    [Node A] Deposit successful.
    Account     : ACC001 — Alice Nakato
    New Balance : UGX 150,000
    [SYNC] -> Node B (192.168.1.20): Success
    [SYNC] -> Node C (192.168.1.30): Success
    -------------------------------------------------------


DEFAULT TEST ACCOUNTS
----------------------
  ACC001 — Alice Nakato    — UGX 100,000
  ACC002 — Bob Mugisha     — UGX 250,000
  ACC003 — Carol Atim      — UGX  75,000
  ACC004 — David Okello    — UGX 500,000
  ACC005 — Eve Namukasa    — UGX  30,000


REPLICATION (3-WAY)
--------------------
After every deposit or withdrawal on any node, sync_service.replicate()
pushes the updated balance to BOTH other nodes via RPC.
With 3 nodes all reachable the client sees:
    [SYNC] -> Node B (...): Success
    [SYNC] -> Node C (...): Success


FAULT TOLERANCE TEST
---------------------
1. Start all three servers.
2. Do a deposit — all three nodes sync.
3. Stop Node C (Ctrl+C on PC 3).
4. Do another deposit.
   -- You will see:
[SYNC] -> ug.mba (...): Success
      [SYNC] -> ug.kam (...): OFFLINE — ...
   -- The transaction commits on ug.hoi and replicates to ug.mba.
   -- ug.kam will be out of sync until it restarts.
5. Restart ug.kam and do a new transaction — the current balance is pushed.

Dijkstra handles the offline node gracefully too:
- During the scan, ug.kam shows UNREACHABLE.
- No edge to ug.kam is added from CLIENT.
- If ug.hoi or ug.mba can still reach ug.kam, Dijkstra finds a relay path.
- If ug.kam is fully isolated, it is shown as UNREACHABLE in the results table.


ADDING A FOURTH NODE (Scalability)
------------------------------------
1. Get the IP of PC 4.
2. In config.py on ALL PCs, add:

       NODE_D = {
           "name":     "Node D",
           "location": "Gulu",
           "host":     "192.168.1.40",
           "port":     18861,
       }

       NODES = {
           "ug.hoi": NODE_HOI,
           "ug.mba": NODE_MBA,
           "ug.kam": NODE_KAM,
           "ug.gul": NODE_GUL,    <-- add this line
       }

3. On PC 4 set THIS_NODE = "D" and run node_server.py.
4. No other code changes required. Dijkstra, sync_service, and
   get_neighbour_latencies all loop over NODES automatically.


FIREWALL NOTE (Windows)
------------------------
If PCs cannot connect, allow port 18861 through Windows Firewall:

    netsh advfirewall firewall add rule ^
        name="MobileMoney RPC" ^
        dir=in action=allow protocol=TCP localport=18861

Or: Windows Security > Firewall > Advanced Settings >
    Inbound Rules > New Rule > Port > TCP 18861 > Allow.

Run this command on every PC that runs node_server.py.


TRANSACTION LOG
----------------
Every deposit, withdrawal, and sync event is appended to:
    transactions.log
Created automatically in the MobileMoney folder on each server PC.


ARCHITECTURE SUMMARY
---------------------

  Client startup (Dijkstra pipeline):
    client.py
      |-- _scan_client_to_nodes()          Measure ms to each node via rpyc
      |-- _fetch_neighbour_latencies()     Ask each node for its inter-node ms
      |-- _build_graph()                   Adjacency dict with ms edge weights
      |-- _dijkstra("CLIENT")              Min-heap shortest path
      |-- auto_connect()                   Open final rpyc conn to best node

  Transaction flow:
    client.py  --[RPC]--> node_server.py (deposit / withdraw)
                               |-- accounts.json  (read/write with threading.Lock)
                               |-- sync_service.replicate()
                                       |--[RPC]--> Node B exposed_update_balance
                                       |--[RPC]--> Node C exposed_update_balance

  Distributed System Properties:
    Replication      All three nodes hold a copy of every balance
    Fault Tolerance  Offline nodes are skipped; local commit is kept
    Transparency     Client calls deposit() like a local function
    Auto-Routing     Dijkstra selects nearest node — no user location input
    Consistency      After sync, all online nodes have the same balance
    Concurrency      ThreadedServer + per-operation threading.Lock
    Openness         Standard rpyc; any Python 3 machine can connect
    Scalability      Add a node in config.py only — zero server code changes

===============================================================================
