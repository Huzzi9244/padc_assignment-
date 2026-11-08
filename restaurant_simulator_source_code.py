"""Concurrent eatery simulation utilizing Python threading primitives."""

from __future__ import annotations

import argparse
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from queue import Queue
from typing import Callable


@dataclass
class Ticket:
    id_num: int
    patron_name: str
    delivered: threading.Event


class Metrics:
    """Manages shared application state using Lock and RLock."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.rlock = threading.RLock()
        self.generated = 0
        self.prepared = 0
        self.delivered = 0

    def ticket_generated(self) -> None:
        with self.lock:
            self.generated += 1

    def _bump_counter(self, attribute_name: str) -> None:
        setattr(self, attribute_name, getattr(self, attribute_name) + 1)

    def ticket_prepared(self) -> None:
        with self.rlock:
            self._bump_counter("prepared")

    def ticket_delivered(self) -> None:
        with self.rlock:
            self._bump_counter("delivered")

    def get_summary(self) -> tuple[int, int, int]:
        with self.lock:
            return self.generated, self.prepared, self.delivered


class Patron(threading.Thread):
    def __init__(
        self,
        identifier: int,
        ticket_queue: Queue[Ticket | None],
        seating: threading.Semaphore,
        metrics: Metrics,
        logger: Callable[[str], None],
    ) -> None:
        super().__init__(name=f"Patron-{identifier}")
        self.identifier = identifier
        self.ticket_queue = ticket_queue
        self.seating = seating
        self.metrics = metrics
        self.logger = logger

    def run(self) -> None:
        my_ticket = Ticket(self.identifier, self.name, threading.Event())
        self.seating.acquire()
        try:
            self.logger(f"{self.name} secured a seat")
            self.metrics.ticket_generated()
            self.ticket_queue.put(my_ticket)
            my_ticket.delivered.wait()
            self.logger(f"{self.name} enjoyed ticket #{my_ticket.id_num}")
        finally:
            self.seating.release()


class Cook(threading.Thread):
    def __init__(
        self,
        identifier: int,
        ticket_queue: Queue[Ticket | None],
        food_window: deque[Ticket],
        window_cond: threading.Condition,
        window_limit: int,
        metrics: Metrics,
        logger: Callable[[str], None],
        sync_barrier: threading.Barrier,
    ) -> None:
        super().__init__(name=f"Cook-{identifier}")
        self.ticket_queue = ticket_queue
        self.food_window = food_window
        self.window_cond = window_cond
        self.window_limit = window_limit
        self.metrics = metrics
        self.logger = logger
        self.sync_barrier = sync_barrier

    def run(self) -> None:
        self.sync_barrier.wait()
        while True:
            current_ticket = self.ticket_queue.get()
            try:
                if current_ticket is None:
                    return
                self.logger(
                    f"{self.name} (pid={os.getpid()}) preparing ticket #{current_ticket.id_num}"
                )
                time.sleep(0.03 + (current_ticket.id_num % 3) * 0.01)
                self.metrics.ticket_prepared()
                
                with self.window_cond:
                    while len(self.food_window) >= self.window_limit:
                        self.window_cond.wait()
                    self.food_window.append(current_ticket)
                    self.window_cond.notify_all()
            finally:
                self.ticket_queue.task_done()


class Server(threading.Thread):
    def __init__(
        self,
        identifier: int,
        food_window: deque[Ticket],
        window_cond: threading.Condition,
        shift_over: threading.Event,
        metrics: Metrics,
        logger: Callable[[str], None],
        sync_barrier: threading.Barrier,
    ) -> None:
        super().__init__(name=f"Server-{identifier}")
        self.food_window = food_window
        self.window_cond = window_cond
        self.shift_over = shift_over
        self.metrics = metrics
        self.logger = logger
        self.sync_barrier = sync_barrier

    def run(self) -> None:
        self.sync_barrier.wait()
        while True:
            with self.window_cond:
                while not self.food_window and not self.shift_over.is_set():
                    self.window_cond.wait()
                if not self.food_window and self.shift_over.is_set():
                    return
                ready_meal = self.food_window.popleft()
                self.window_cond.notify_all()
                
            time.sleep(0.015)
            self.metrics.ticket_delivered()
            self.logger(f"{self.name} delivered ticket #{ready_meal.id_num}")
            ready_meal.delivered.set()


def execute_sim(cook_qty: int, patron_qty: int, verbose_output: bool = True) -> float:
    start_timer = time.perf_counter()
    ticket_queue: Queue[Ticket | None] = Queue()
    seating = threading.Semaphore(3)
    food_window: deque[Ticket] = deque()
    window_cond = threading.Condition()
    shift_over = threading.Event()
    doors_open = threading.Event()
    sim_metrics = Metrics()
    console_lock = threading.Lock()

    def thread_safe_print(msg: str) -> None:
        if verbose_output:
            with console_lock:
                print(msg)

    staff_ready = threading.Barrier(cook_qty + 2, action=doors_open.set)
    
    cook_team = [
        Cook(i, ticket_queue, food_window, window_cond, 5, sim_metrics, thread_safe_print, staff_ready)
        for i in range(1, cook_qty + 1)
    ]
    
    server_team = [
        Server(i, food_window, window_cond, shift_over, sim_metrics, thread_safe_print, staff_ready)
        for i in range(1, 3)
    ]

    # Wait for all workers to hit the synchronization barrier
    for worker in cook_team + server_team:
        worker.start()
        
    doors_open.wait()
    
    patron_group = [
        Patron(i, ticket_queue, seating, sim_metrics, thread_safe_print)
        for i in range(1, patron_qty + 1)
    ]
    
    for p in patron_group:
        p.start()
        time.sleep(0.005)
        
    for p in patron_group:
        p.join()

    # Dispatch sentinel markers to signal end of work for cooks
    for _ in cook_team:
        ticket_queue.put(None)
        
    for c in cook_team:
        c.join()

    shift_over.set()
    with window_cond:
        window_cond.notify_all()
        
    for s in server_team:
        s.join()

    duration = time.perf_counter() - start_timer
    generated, prepared, delivered = sim_metrics.get_summary()
    
    if (generated, prepared, delivered) != (patron_qty, patron_qty, patron_qty):
        raise RuntimeError(
            f"Simulation integrity failure: generated={generated}, prepared={prepared}, delivered={delivered}"
        )
        
    return duration


def main() -> None:
    arg_parser = argparse.ArgumentParser(description="Concurrent eatery threading simulation")
    arg_parser.add_argument("--patrons", type=int, default=10)
    parsed_args = arg_parser.parse_args()
    
    if parsed_args.patrons < 1:
        arg_parser.error("--patrons flag requires a minimum value of 1")

    print("Eatery is now open with 3 cooks, 2 servers, and 3 seats")
    multi_cook_duration = execute_sim(3, parsed_args.patrons)
    single_cook_duration = execute_sim(1, parsed_args.patrons, verbose_output=False)
    
    print(f"\n1 cook:  {single_cook_duration:.3f} seconds")
    print(f"3 cooks: {multi_cook_duration:.3f} seconds")


if __name__ == "__main__":
    main()