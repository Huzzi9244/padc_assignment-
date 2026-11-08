# Eatery Threading Simulation

This project illustrates core concurrent programming and thread synchronization concepts by modeling a restaurant environment via a command-line interface.

The simulation models the following entities:
- **Patrons (Customers)** who arrive, place meal tickets, and wait to be served.
- **Cooks (Chefs)** who process and prepare the meal tickets.
- **Servers (Waiters)** who pick up prepared meals and deliver them to the patrons.
- A dining area with a limited capacity of three seats.
- A food window (buffer) that can hold a maximum of five prepared meals at a time.

## Execution Instructions

Navigate to the project directory and run the script using Python:

```powershell
python restaurant_sim.py
```

By default, the simulation processes 10 patrons. You can specify a different number of patrons using the command-line argument:

```powershell
python restaurant_sim.py --patrons 20
```

The script will execute the simulation and output a performance comparison between a single-cook setup and a three-cook setup.

## Synchronization Primitives Used

### Thread Subclasses
The `Patron`, `Cook`, and `Server` classes inherit directly from `threading.Thread`. Their concurrent behaviors are defined within their overridden `run()` methods.

### Queues
Patrons submit `Ticket` objects into a thread-safe `Queue`. Cooks retrieve these tickets for processing. The `task_done()` method is utilized to track when a ticket has been fully handled.

### Semaphores
The dining capacity is managed via a Semaphore:
```python
seating = threading.Semaphore(3)
```
This ensures a maximum of three patrons can be seated concurrently. A patron must `acquire()` a seat before eating and will `release()` it upon departure.

### Locks (Mutex)
A standard `Lock` (`Metrics.lock`) is used to safely increment the total number of generated tickets. Additionally, an output lock guarantees that print statements from various threads do not overlap or scramble in the console.

### Reentrant Locks (RLock)
`Metrics.rlock` safeguards the counters for prepared and delivered meals. An `RLock` allows the same thread to acquire the lock multiple times, accommodating nested method calls that might share synchronization requirements.

### Condition Variables
Cooks place finished meals into a `deque` acting as the food window, and servers pick them up from there. A `Condition` variable forces cooks to wait if the food window reaches its capacity, and forces servers to wait if the window is empty. The traditional `while` loop pattern is used around the `wait()` calls to prevent spurious wakeups.

### Events
Each meal ticket has an associated `Event`. A patron blocks using `ticket.delivered.wait()` until a server finalizes the delivery via `ticket.delivered.set()`. 
Furthermore, a `doors_open` event indicates when all staff are ready, and a `shift_over` event signals servers to stop expecting new meal tickets.

### Barriers
To synchronize the start of the simulation, cooks and servers wait at a `Barrier`. Patrons are only allowed in once all staff members have synchronized at this starting line.

### Thread Naming and Process IDs
Threads are assigned descriptive names (e.g., `Patron-1`, `Cook-1`). The cooks also output the current process ID (`os.getpid()`) to demonstrate that all threads operate under a single shared process.

### Joining Threads
The main application thread uses `join()` on all active patrons, cooks, and servers to block the main program from terminating until all concurrent operations are finalized.

## Simulation Lifecycle

1. Initialize synchronization tools: ticket queue, seating semaphore, food window, events, barrier, and shared metrics.
2. Spin up all cook and server threads.
3. Staff synchronize at the barrier, triggering the eatery to open.
4. Patrons grab seats and submit their meal tickets to the queue.
5. Cooks process the tickets and place meals in the food window.
6. Servers fetch meals from the window and notify the waiting patrons.
7. Patrons finish their meals, vacate their seats, and leave.
8. Sentinel markers (`None`) are passed to cooks to signal the end of the tickets.
9. A `shift_over` event is broadcasted to gracefully shut down the servers.
10. The system asserts that the number of generated, prepared, and delivered tickets match perfectly.
11. Finally, execution times for different staff configurations are compared.

## Sample Output

```text
Eatery is now open with 3 cooks, 2 servers, and 3 seats
Patron-1 secured a seat
Cook-1 (pid=98765) preparing ticket #1
Server-2 delivered ticket #1
Patron-1 enjoyed ticket #1

1 cook:  0.436 seconds
3 cooks: 0.234 seconds
```
*(Note: Because threads are scheduled concurrently by the OS, the exact sequence of log messages will vary between runs.)*
