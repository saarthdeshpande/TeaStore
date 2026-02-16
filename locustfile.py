#!/usr/bin/python
#
# TeaStore Locust load generator (Poisson replay + RPS logging)
#
# Adapted from Online Boutique Locustfile: replaces endpoints with TeaStore
# equivalents while keeping the same CSV-driven Poisson replay and RPS export.
#

import glob
import math
import os
import random
import threading
from collections import defaultdict

from locust import FastHttpUser, TaskSet, between, events, task
from faker import Faker
import csv
import datetime
import json
import logging
import socket
import time

logging.basicConfig(level=logging.ERROR)

locust_environment = None
service_files_dir = None
real_time = False
last_record_time = time.time()

# --- RPS replay globals ---
rps_schedules = {}        # endpoint_name -> list of rps values (per 15s bucket)
replay_interval = 15.0    # seconds per CSV row
SCALE_FACTOR = 0.03       # scale factor for RPS values in CSVs

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "-d", "--dir",
        type=str,
        default="./cpu_memory_",
        help="Directory to write rps.txt / rt_rps.txt"
    )
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="Enable real-time RPS/p95/p99 socket export"
    )

@events.test_start.add_listener
def _(environment, **kwargs):
    print(f"Service files directory: {service_files_dir}")
    print(f"Realtime: {real_time}")

send_every = 0  # seconds (0 = send every time threshold is hit)
start_time = None

def send_dict_via_socket(dictionary, host='localhost', port=8001):
    """
    Send current per-endpoint RPS/p95/p99 as JSON over TCP.
    Kept identical to your original code.
    """
    global start_time
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if len(dictionary) == 18:  # magic number from your original script
        try:
            if start_time is None:
                start_time = time.time()
            elif time.time() >= start_time + send_every:
                start_time = time.time()
                client.connect((host, port))

                json_data = json.dumps(dictionary).encode('utf-8')
                client.send(json_data)

                response = client.recv(1024).decode('utf-8')
                print(f"Server response: {response}")
        except ConnectionRefusedError as cre:
            logging.info(cre)
        finally:
            client.close()

def record_rps():
    """
    Periodically writes RPS + latency percentiles per endpoint to a text file,
    and optionally sends real-time stats via socket.
    """
    global locust_environment, last_record_time, service_files_dir, real_time
    if locust_environment:
        stats = locust_environment.runner.stats
        filename = "rps.txt"
        if real_time is True:
            filename = "rt_rps.txt"
            real_time_stats = {
                key: value
                for endpoint, data in stats.entries.items()
                for key, value in [
                    (f"{endpoint}_rps", data.current_rps),
                    (f"{endpoint}_p95", data.get_response_time_percentile(0.95)),
                    (f"{endpoint}_p99", data.get_response_time_percentile(0.99))
                ]
            }
            send_dict_via_socket(real_time_stats)

        with open(f"{service_files_dir}/{filename}", "a") as rps_output_file:
            for endpoint, data in stats.entries.items():
                rps = data.current_rps
                p95 = data.get_response_time_percentile(0.95)
                p99 = data.get_response_time_percentile(0.99)
                rps_output_file.write(f"{endpoint}_rps: {rps};")
                rps_output_file.write(f"{endpoint}_p95: {p95};")
                rps_output_file.write(f"{endpoint}_p99: {p99};")
            rps_output_file.write("\n")
            rps_output_file.close()

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """
    Store environment, start the periodic RPS writer.
    """
    global last_record_time, locust_environment, service_files_dir, real_time
    service_files_dir = environment.parsed_options.dir
    real_time = environment.parsed_options.realtime
    last_record_time = time.time()
    locust_environment = environment

    t = threading.Thread(target=periodic_rps_writer, daemon=True)
    t.start()

request_latencies = defaultdict(list)
write_lock = threading.Lock()

fake = Faker()

# ---------------------------------------------------------------------------
# TeaStore-specific endpoint behaviors
#
# IMPORTANT: --host should be set to the full WebUI base, e.g.:
#   --host http://<NODE_IP>:30080/tools.descartes.teastore.webui
# so that these relative paths work as expected.
#
# TeaStore URLs are based on published usage:
#   /                  (home)
#   /category?category=2&page=1
#   /product?id=18
#   /cart
#   /cartAction/productid=113&addToCart=Add+to+Cart
#   /login, /loginAction/logout, etc. :contentReference[oaicite:3]{index=3}
# ---------------------------------------------------------------------------

# Choose reasonable ranges. You can tune these based on your DB size.
CATEGORY_IDS = [0, 1, 2, 3, 4, 5]
PRODUCT_IDS = list(range(1, 200))  # adjust to your dataset
PAGES = [1, 2, 3]

BASE_PATH = "/tools.descartes.teastore.webui"

def index(l):
    l.client.get(f"{BASE_PATH}/", name="index")

def browseCategory(l):
    category = random.choice(CATEGORY_IDS)
    page = random.choice(PAGES)
    l.client.get(
        f"{BASE_PATH}/category?category={category}&page={page}",
        name="browseCategory",
    )

def viewProduct(l):
    product_id = random.choice(PRODUCT_IDS)
    l.client.get(f"{BASE_PATH}/product?id={product_id}", name="viewProduct")

def viewCart(l):
    l.client.get(f"{BASE_PATH}/cart", name="viewCart")

def addToCart(l):
    product_id = random.choice(PRODUCT_IDS)
    l.client.post(
        f"{BASE_PATH}/cartAction/productid={product_id}&addToCart=Add+to+Cart",
        name="addToCart",
    )

def checkout(l):
    viewCart(l)
    l.client.post(f"{BASE_PATH}/cartAction",
                  {"proceedToCheckout": "Checkout"},
                  name="checkout")
    l.client.post(f"{BASE_PATH}/cartAction",
                  {"confirmOrder": "Confirm"},
                  name="checkout")

def login(l):
    l.client.get(f"{BASE_PATH}/login", name="login")
    l.client.post(
        f"{BASE_PATH}/loginAction",
        {
            "username": "user",
            "password": "password",
        },
        name="loginAction",
    )

def logout(l):
    l.client.post(f"{BASE_PATH}/loginAction/logout", name="logout")
# ---------------------------------------------------------------------------
# Poisson replay logic (unchanged from your Online Boutique script)
# ---------------------------------------------------------------------------

import gevent

def _call_endpoint(user_taskset, endpoint_name):
    """
    Map endpoint_name (from rps_schedules keys) to TeaStore actions.
    """
    if endpoint_name == "index":
        index(user_taskset)
    elif endpoint_name == "browseCategory":
        browseCategory(user_taskset)
    elif endpoint_name == "viewProduct":
        viewProduct(user_taskset)
    elif endpoint_name == "addToCart":
        addToCart(user_taskset)
    elif endpoint_name == "viewCart":
        viewCart(user_taskset)
    elif endpoint_name == "checkout":
        checkout(user_taskset)
    else:
        # Unknown endpoint; ignore
        print(f"Ignoring endpoint = {endpoint_name}")
        pass

def _drive_endpoint(user_taskset, endpoint_name, series, t0):
    """
    Drives one endpoint according to its RPS time series using
    a Poisson process (exponential inter-arrival times).
    """
    for i, rps in enumerate(series):
        lam = rps
        while time.time() - t0 < (i + 1) * replay_interval:
            u = random.random()
            if u == 0:
                u = 1e-9

            inter_arrival_time = -math.log(u) / lam
            gevent.sleep(inter_arrival_time)

            if time.time() - t0 >= (i + 1) * replay_interval:
                break
            try:
                _call_endpoint(user_taskset, endpoint_name)
            except Exception as e:
                logging.info(e)

def replay_poisson(user_taskset):
    """
    Spawns one greenlet per endpoint, each replaying its schedule.
    """
    if not rps_schedules:
        return

    t0 = time.time()
    greens = []
    for endpoint, series in rps_schedules.items():
        if not series or all(r <= 0 for r in series):
            print(f"Endpoint {endpoint} has no valid schedule; skipping")
            continue
        g = gevent.spawn(_drive_endpoint, user_taskset, endpoint, series, t0)
        greens.append(g)

    gevent.joinall(greens)
    for g in greens:
        if not g.ready():
            g.kill()

def load_rps_files(dir_path):
    """
    Loads 6 CSV files (timestamp,rps) and maps them to TeaStore endpoints.

    Expected CSV format:
      header includes "rps"
      each row = one 15s bucket

    Mapping (by sorted filename index):
      0 -> index
      1 -> browseCategory
      2 -> viewProduct
      3 -> addToCart
      4 -> viewCart
      5 -> checkout
    """
    files = sorted(glob.glob("alibaba_workload/alibaba_*.csv"))
    csv_files = [f for f in files if os.path.isfile(f)]
    if len(csv_files) < 6:
        raise FileNotFoundError(
            f"WARNING: expected 6 RPS CSV files, found {len(csv_files)} in {dir_path}"
        )

    target_endpoints = [
        "index",
        "browseCategory",
        "viewProduct",
        "addToCart",
        "viewCart",
        "checkout",
    ]
    for i, endpoint in enumerate(target_endpoints):
        if i >= len(csv_files):
            rps_schedules[endpoint] = []
            continue
        path = csv_files[i]
        rows = []
        with open(path, newline="") as fh:
            reader = csv.DictReader(fh)
            for _ in range(1440):
                next(reader, None)
            for row in reader:
                rps = float(row["rps"]) * SCALE_FACTOR
                rows.append(rps)
        rps_schedules[endpoint] = rows

    max_len = max((len(v) for v in rps_schedules.values()), default=0)
    return max_len

def periodic_rps_writer():
    """
    Background thread to periodically call record_rps().
    """
    global locust_environment, service_files_dir, real_time
    while True:
        try:
            record_rps()
            time.sleep(15)
        except Exception as e:
            logging.info(f"rps writer error: {e}")

# ---------------------------------------------------------------------------
# Locust user behavior
# ---------------------------------------------------------------------------

class UserBehavior(TaskSet):

    def on_start(self):
        """
        On start:
          - load RPS CSVs
          - warm up with index()
          - start Poisson replay for this user
        """
        load_rps_files("alibaba_workload")
        index(self)
        replay_poisson(self)

    @task
    def _idle(self):
        """
        Keep Locust happy by having at least one @task.
        All real work is done in the replay greenlets.
        """
        time.sleep(3600)

class WebsiteUser(FastHttpUser):
    tasks = [UserBehavior]
    wait_time = between(0, 0)
