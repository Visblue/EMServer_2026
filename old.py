import threading
import time
import random
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import InvalidOperation
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder
from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusException
import logging

logging.getLogger("pymodbus").setLevel(logging.CRITICAL)

# --------------------------------------------------------------------
# KONSTANTER FOR INFRASTRUKTUR
# --------------------------------------------------------------------

# Fremover hentes devices fra MongoDB:
#   DB: Meters
#   Collection: devices
METERS_DB = "Meters"
METERS_DEVICES_COLLECTION = "devices"

# Energy logs gemmes stadig i denne DB.
ENERGY_LOGS_DB = "customer_energy_logs"

# Servers hentes ikke længere fra MongoDB config.
# Tilpas efter behov (eller gør dem env-styrede senere).
SERVERS = [
    {"ip": "localhost", "port": 650},
]

# --------------------------------------------------------------------
# GLOBALE KONSTANTER
# --------------------------------------------------------------------
SLEEP = random.randint(3,5)  # sek mellem målinger pr. enhed (mål-interval)
DB_SAVE_INTERVAL = 30  # sek mellem database gem operationer
CONFIG_RELOAD_INTERVAL = 300  # 5 minutter mellem config reload fra MongoDB

MAX_REASONABLE_DIFF_WH = 500_000    # 50 kWh pr. ~interval – spikes over det afvises
MAX_REASONABLE_POWER_W = 1_000_000  # 1 MW
FLOAT_TOLERANCE_WH = 0.01
MIN_TOTAL_VALUE = 0
MAX_TOTAL_RESET_THRESHOLD = 0.5     # meter reset hvis ny < 50% af gammel

BACKOFF_MIN = 60     # 1 minut
BACKOFF_MAX = 360    # 6 minutter

SERVER_REGISTERS = {
    "active_power": 19026,
    "total_import": 19068,
    "total_export": 19076,
}

shutdown_event = threading.Event()
error_state = {}  # rate-limited logging: {site: {"count": int, "last_log": ts}}
states_lock = threading.Lock()  # Lock for thread-safe config reload


# --------------------------------------------------------------------
# HJÆLPEKLASSER – MODBUS + CONFIG
# --------------------------------------------------------------------

class ModbusDataConverter:
    """Samler ALLE converters ét sted."""

    def decode_int32_1(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return dec.decode_32bit_int()

    def decode_int16(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return dec.decode_16bit_int()

    def decode_uint16(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return dec.decode_16bit_uint()

    def decode_uint32(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return dec.decode_32bit_uint()

    def decode_int32(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Little
        )
        return dec.decode_32bit_int()

    def decode_float32_1(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return round(dec.decode_32bit_float(), 3)

    def decode_float32_2(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Little
        )
        return round(dec.decode_32bit_float(), 3)

    def decode_float_64(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Big
        )
        return dec.decode_64bit_float()

    def decode_float_64_little(self, registers):
        dec = BinaryPayloadDecoder.fromRegisters(
            registers, byteorder=Endian.Big, wordorder=Endian.Little
        )
        return dec.decode_64bit_float()


class ModbusDataReader(ModbusDataConverter):
    """Wrapper omkring ModbusTcpClient, inkl. read/write helpers."""

    def __init__(self, client, unit_id):
        self.client = client
        self.unit_id = unit_id

    def read_register(self, reg_cfg: dict, *, default_table: str | None = None):
        addr = int(reg_cfg["address"])
        fmt = reg_cfg["format"]
        count = reg_cfg.get("count", 2)

        # Table selection (holding/input)
        # Priority:
        # - per-register override: reg_cfg["reading"] or reg_cfg["table"]
        # - device default (passed in): default_table
        # - legacy behavior: int32_1 uses input registers, everything else uses holding
        table = reg_cfg.get("reading") or reg_cfg.get("table") or default_table
        if table is None:
            table = "input" if fmt == "int32_1" else "holding"
        table = str(table).strip().lower()
        if table not in ("holding", "input"):
            raise ValueError(f"Ukendt register-table: {table} (for {addr})")

        read_fn = self.client.read_input_registers if table == "input" else self.client.read_holding_registers

        if fmt in ("int16", "uint16"):
            res = read_fn(addr, count=1, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if fmt == "int16":
                return self.decode_int16(res.registers)
            else:
                return self.decode_uint16(res.registers)

        elif fmt == "uint32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if len(res.registers) != 2:
                raise ModbusException(f"uint32 expected 2 regs, got {len(res.registers)}")
            return self.decode_uint32(res.registers)

        elif fmt == "int32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32(res.registers)

        elif fmt == "int32_1":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32_1(res.registers)

        elif fmt in ("float32_1", "float32", "float32_2"):
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if fmt in ("float32_1", "float32"):
                return self.decode_float32_1(res.registers)
            else:
                return self.decode_float32_2(res.registers)

        elif fmt == "double":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float_64(res.registers)

        elif fmt == "double_little":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float_64_little(res.registers)

        else:
            raise ValueError(f"Ukendt format: {fmt}")

    def write_float32(self, value: float, address: int, unit_id: int):
        builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
        builder.add_32bit_float(float(value))
        regs = builder.to_registers()
        res = self.client.write_registers(address, regs, unit=unit_id)
        if res.isError():
            raise ModbusException(f"Write error at {address}")


class ConfigLoader:
    """Loader devices fra Mongo: Meters.devices"""

    def __init__(self, uri="mongodb://vbserver:27017"):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[METERS_DB]
        self.coll = self.db[METERS_DEVICES_COLLECTION]

    def load_config(self):
        devices = list(self.coll.find({}, {"_id": 0}))
        if not devices:
            raise ValueError("Ingen devices fundet i MongoDB (Meters.devices)")

        cfg = {"devices": devices, "servers": list(SERVERS)}
        em_groups = {}

        single_devices = []
        for d in devices:
            em_group = d.get("em_group")
            if em_group:
                em_groups.setdefault(em_group, []).append(d)
            else:
                single_devices.append(d)

        if em_groups:
            print(f"[INFO] Fundet {len(em_groups)} em_groups i config.")

        cfg["devices"] = single_devices
        cfg["em_groups"] = em_groups
        return cfg

    def close(self):
        self.client.close()


# --------------------------------------------------------------------
# DEVICE & EM-GROUP STATE
# --------------------------------------------------------------------

def get_collection_name(device: dict) -> str:
    if "data_collection" in device:
        return device["data_collection"]
    project = device.get("project_nr", "")
    name = device.get("name", "device")
    if project:
        return f"{project}_{name}"
    return name


def load_last_totals_from_collection(collection):
    try:
        latest = collection.find_one(
            {},
            {"_id": 0, "Total_Imported": 1, "Total_Exported": 1},
            sort=[("Time", -1)]
        )
        if latest:
            imp = float(latest.get("Total_Imported", 0.0))
            exp = float(latest.get("Total_Exported", 0.0))
            return imp, exp
    except Exception as e:
        print(f"[WARN] {collection.name}: Fejl ved indlæsning af sidste totals: {e}")
    return 0.0, 0.0


class DeviceState:
    """State for én måler + tilhørende server."""
    def __init__(self, device_cfg: dict, server_cfg: dict, mongo_db):
        self.is_group = False
        self.device = device_cfg
        self.server = server_cfg
        self.site = device_cfg.get("site", "unknown")
        self.server_unit_id = device_cfg.get("server_unit_id", 1)

        coll_name = get_collection_name(device_cfg)
        self.collection = mongo_db[coll_name]
        self.collection.create_index([("Time", -1)], background=True)

        self.device_client: ModbusDataReader | None = None
        self.server_client: ModbusDataReader | None = None

        self.last_import, self.last_export = load_last_totals_from_collection(self.collection)
        self.first_read = True  # Første læsning efter restart

        self.consecutive_errors = 0
        self.backoff_until = 0.0
        self.next_run_at = time.time() + random.uniform(0, SLEEP)
        self.last_run_at = 0.0
        self.last_db_save = 0.0  # Sidste gang vi gemte i database

        self.run_count = 0
        self.last_debug_print = 0.0


class EmGroupState:
    """State for en EM-gruppe (flere EM’er, én samlet server/DB)."""
    def __init__(self, group_name: str, devices: list[dict], server_cfg: dict, mongo_db):
        self.is_group = True
        self.group_name = group_name
        self.server = server_cfg
        self.server_unit_id = devices[0].get("server_unit_id", 1)
        self.site = f"EM_GROUP:{group_name}"

        first = devices[0]
        if "data_collection" in first:
            coll_name = first["data_collection"]
        else:
            proj = first.get("project_nr", "")
            coll_name = f"{proj}_{group_name}" if proj else group_name

        self.collection = mongo_db[coll_name]
        self.collection.create_index([("Time", -1)], background=True)

        self.server_client: ModbusDataReader | None = None

        # Per-EM state
        self.ems = []
        for d in devices:
            self.ems.append({
                "cfg": d,
                "reader": None,
                "consecutive_errors": 0,
                "backoff_until": 0.0,
            })

        self.last_import, self.last_export = load_last_totals_from_collection(self.collection)

        self.consecutive_errors = 0  # gruppe-niveau (ingen EM’er virker)
        self.backoff_until = 0.0
        self.next_run_at = time.time() + random.uniform(0, SLEEP)
        self.last_run_at = 0.0
        self.last_db_save = 0.0  # Sidste gang vi gemte i database

        self.run_count = 0
        self.last_debug_print = 0.0


# --------------------------------------------------------------------
# GENERELLE HJÆLPERE
# --------------------------------------------------------------------

def rate_limited_log(site: str, msg: str, interval: int = 60):
    now = time.time()
    state = error_state.setdefault(site, {"count": 0, "last_log": 0})
    state["count"] += 1
    if now - state["last_log"] >= interval:
        print(f"{site}: {msg} (fejl #{state['count']})")
        state["last_log"] = now


def connect_modbus_client(ip: str, port: int, timeout: int = 1):
    """Helper til at lave ModbusTcpClient med kort timeout (1s)."""
    try:
        client = ModbusTcpClient(ip, port=port, timeout=timeout, retries=1, retry_on_empty=True)
        if client.connect():
            return client
        client.close()
    except Exception:
        pass
    return None


def validate_total_value(new: float, last: float, value_name: str, first_read: bool = False):
    """
    Returnerer (is_valid, validated_value, reason)
    validated_value er typisk:
      - new (hvis OK)
      - last (hvis vi afviser new)
    """
    if new < MIN_TOTAL_VALUE:
        return False, last, f"{value_name} negativ ({new})"

    # Første læsning efter restart - accepter altid (device totals kan være højere)
    if first_read:
        return True, new, "første læsning efter restart"

    # Første læsning eller meget lav sidste værdi - accepter altid
    if last <= 1.0:  # Under 1 Wh betragtes som "ingen tidligere data"
        return True, new, "første læsning"

    if new < last * MAX_TOTAL_RESET_THRESHOLD:
        return True, new, "meter reset"

    diff = new - last

    if diff < -FLOAT_TOLERANCE_WH:
        return False, last, f"{value_name} faldt ({last} -> {new})"

    # Tillad større spike ved lave værdier (fx første par målinger efter installation)
    # Under 10 kWh (10,000 Wh) tillades op til 10x normal spike threshold
    if last < 10000 and diff > MAX_REASONABLE_DIFF_WH:
        extended_limit = MAX_REASONABLE_DIFF_WH * 10
        if diff > extended_limit:
            return False, last, f"{value_name} spike ({diff} Wh) selv med udvidet grænse"
        return True, new, f"OK (tidlig fase, diff={diff} Wh)"

    if diff > MAX_REASONABLE_DIFF_WH:
        return False, last, f"{value_name} spike ({diff} Wh)"

    return True, new, "OK"


def safe_write(server_client: ModbusDataReader, value, reg_key: str, unit_id: int):
    if value is None:
        return
    try:
        if value != value:
            return
        if abs(value) == float("inf"):
            return
        if not isinstance(value, (int, float)):
            return
        server_client.write_float32(value, SERVER_REGISTERS[reg_key], unit_id)
    except Exception as e:
        print(f"[ERROR] Fejl ved skrivning til {reg_key}: {e}")


def close_modbus_reader(reader: ModbusDataReader | None):
    if reader and reader.client:
        try:
            reader.client.close()
        except Exception:
            pass


# --------------------------------------------------------------------
# ENERGI-READ + META (invalid info til frontend)
# --------------------------------------------------------------------

def read_energy_registers(reader: ModbusDataReader,
                          device_cfg: dict,
                          last_import: float,
                          last_export: float,
                          interval_sec: float,
                          first_read: bool = False):
    """
    Returnerer (active_power, total_import, total_export, meta)
    interval_sec bruges til AP-only fallback.

    Offsets tilføjes per-register via registers.<name>.offset (i post-scale enheder).
    """
    regs = device_cfg["registers"]
    site = device_cfg.get("site", "unknown")

    has_ap = "active_power" in regs
    has_imp = "total_import" in regs
    has_exp = "total_export" in regs

    active_power = 0.0
    total_import = last_import
    total_export = last_export

    meta = {
        "active_power_invalid": False,
        "active_power_raw": None,
        "active_power_reason": "",
        "import_invalid": False,
        "import_raw": None,
        "import_reason": "",
        "export_invalid": False,
        "export_raw": None,
        "export_reason": "",
    }

    # Device-level default read type (holding/input). Can be overridden per-register.
    default_table = str(device_cfg.get("reading") or device_cfg.get("table") or "holding").strip().lower()

    def read_reg(reg_cfg: dict):
        return reader.read_register(reg_cfg, default_table=default_table)

    if has_ap:
        raw = read_reg(regs["active_power"])
        scale = regs["active_power"].get("scale", 1.0)
        ap_raw = float(raw) * float(scale)

        # Offsets are applied AFTER scaling.
        # Use registers.active_power.offset
        ap_offset = regs["active_power"].get("offset", 0.0) or 0.0
        ap_raw = ap_raw + float(ap_offset)

        meta["active_power_raw"] = ap_raw

        if ap_raw != ap_raw or abs(ap_raw) > MAX_REASONABLE_POWER_W:
            meta["active_power_invalid"] = True
            meta["active_power_reason"] = f"Urealistisk active_power: {ap_raw}"
            rate_limited_log(site, f"Active_power invalid: {ap_raw}")
            active_power = 0.0
        else:
            active_power = ap_raw

    if has_imp:
        raw = read_reg(regs["total_import"])
        scale = regs["total_import"].get("scale", 1.0)
        new_val = round(float(raw) * float(scale), 3)
        imp_offset = regs["total_import"].get("offset")
        if imp_offset is not None:
            new_val = round(new_val + float(imp_offset), 3)
        meta["import_raw"] = new_val
        ok, validated, reason = validate_total_value(new_val, last_import, "Import", first_read)
        if not ok:
            meta["import_invalid"] = True
            meta["import_reason"] = reason
            rate_limited_log(site, f"Import invalid: {reason}")
        total_import = validated

    if has_exp:
        raw = read_reg(regs["total_export"])
        scale = regs["total_export"].get("scale", 1.0)
        new_val = round(float(raw) * float(scale), 3)
        exp_offset = regs["total_export"].get("offset")
        if exp_offset is not None:
            new_val = round(new_val + float(exp_offset), 3)
        meta["export_raw"] = new_val
        ok, validated, reason = validate_total_value(new_val, last_export, "Export", first_read)
        if not ok:
            meta["export_invalid"] = True
            meta["export_reason"] = reason
            rate_limited_log(site, f"Export invalid: {reason}")
        total_export = validated

    if has_ap and not (has_imp or has_exp):
        effective_interval = max(interval_sec, 1.0)
        energy_wh = (active_power * effective_interval) / 3600.0
        if active_power > 0:
            total_import = last_import + energy_wh
        elif active_power < 0:
            total_export = last_export + abs(energy_wh)

    return active_power, total_import, total_export, meta


# --------------------------------------------------------------------
# GEM TIL DB (inkl. invalid flag til frontend)
# --------------------------------------------------------------------

def save_data_to_db(device: dict,
                    collection,
                    last_import: float,
                    last_export: float,
                    active_power: float,
                    total_import: float,
                    total_export: float,
                    meta: dict):
    site = device.get("site", "unknown")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if active_power != active_power:
        active_power = 0.0
    if total_import != total_import:
        total_import = last_import
    if total_export != total_export:
        total_export = last_export

    imp_diff = max(0, total_import - last_import) if total_import is not None else 0
    exp_diff = max(0, total_export - last_export) if total_export is not None else 0

    doc = {
        "Site": site,
        "Project_nr": device.get("project_nr", ""),
        "Device_name": device.get("name", ""),
        "Time": now,
        "Active_power": round(active_power, 2),
        "Imported_Wh": imp_diff,
        "Exported_Wh": exp_diff,
        "Total_Imported": total_import,
        "Total_Exported": total_export,
        "Total_Imported_kWh": total_import / 1000,
        "Total_Exported_kWh": total_export / 1000,
        "Active_power_valid": not meta.get("active_power_invalid", False),
        "Active_power_raw": meta.get("active_power_raw"),
        "Active_power_invalid_reason": meta.get("active_power_reason", ""),
        "Import_valid": not meta.get("import_invalid", False),
        "Import_raw": meta.get("import_raw"),
        "Import_invalid_reason": meta.get("import_reason", ""),
        "Export_valid": not meta.get("export_invalid", False),
        "Export_raw": meta.get("export_raw"),
        "Export_invalid_reason": meta.get("export_reason", ""),
    }

    doc["Has_invalid_data"] = (
        meta.get("active_power_invalid", False)
        or meta.get("import_invalid", False)
        or meta.get("export_invalid", False)
    )

    try:
        collection.insert_one(doc)
    except InvalidOperation:
        print(f"💥 {site}: Mongo client lukket – kan ikke gemme")
        return last_import, last_export
    except Exception as e:
        rate_limited_log(site, f"MongoDB fejl: {e}")
        return last_import, last_export

    return total_import, total_export


# --------------------------------------------------------------------
# POLL ENKELT DEVICE
# --------------------------------------------------------------------

def poll_device_once(state: DeviceState):
    start_ts = time.time()
    now = start_ts
    if now < state.backoff_until:
        return

    site = state.site
    dev = state.device
    srv = state.server

    if state.last_run_at > 0:
        actual_interval = now - state.last_run_at
    else:
        actual_interval = SLEEP

    if not state.device_client or not state.device_client.client.is_socket_open():
        ip = dev["ip"]
        port = dev["port"]
        tcp = connect_modbus_client(ip, port)
        if tcp:
            state.device_client = ModbusDataReader(tcp, dev["unit_id"])
            print(f"✅ {site}: device forbundet ({ip}:{port})")
        else:
            rate_limited_log(site, "kan ikke forbinde til device")
            state.consecutive_errors += 1
            if state.consecutive_errors >= 3:
                backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                print(f"⏸️ {site}: backoff {backoff}s pga. gentagne device-fejl")
                state.backoff_until = now + backoff
                state.consecutive_errors = 0
                close_modbus_reader(state.device_client)
                state.device_client = None
            return

    if not state.server_client or not state.server_client.client.is_socket_open():
        ip = srv.get("ip", "localhost")
        port = srv.get("port", 650)
        tcp = connect_modbus_client(ip, port)
        if tcp:
            state.server_client = ModbusDataReader(tcp, 1)
            print(f"✅ {site}: server forbundet ({ip}:{port})")
        else:
            rate_limited_log(site, "kan ikke forbinde til server")
            state.consecutive_errors += 1
            if state.consecutive_errors >= 3:
                backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                print(f"⏸️ {site}: backoff {backoff}s pga. gentagne server-fejl")
                state.backoff_until = now + backoff
                state.consecutive_errors = 0
                close_modbus_reader(state.server_client)
                state.server_client = None
            return

    try:
        ap, total_imp, total_exp, meta = read_energy_registers(
            state.device_client,
            dev,
            state.last_import,
            state.last_export,
            actual_interval,
            state.first_read
        )
        state.first_read = False  # Kun første gang
    except ModbusException as e:
        rate_limited_log(site, f"Modbus fejl: {e}")
        state.consecutive_errors += 1
        close_modbus_reader(state.device_client)
        state.device_client = None
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
            print(f"⏸️ {site}: backoff {backoff}s pga. gentagne Modbus-fejl")
            state.backoff_until = now + backoff
            state.consecutive_errors = 0
        return
    except Exception as e:
        rate_limited_log(site, f"Uventet fejl i read: {e}")
        state.consecutive_errors += 1
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
            print(f"⏸️ {site}: backoff {backoff}s pga. gentagne fejl")
            state.backoff_until = now + backoff
            state.consecutive_errors = 0
        return

    try:
        safe_write(state.server_client, ap, "active_power", state.server_unit_id)
        if abs(total_imp - state.last_import) > FLOAT_TOLERANCE_WH:
            safe_write(state.server_client, total_imp, "total_import", state.server_unit_id)
        if abs(total_exp - state.last_export) > FLOAT_TOLERANCE_WH:
            safe_write(state.server_client, total_exp, "total_export", state.server_unit_id)
    except Exception as e:
        rate_limited_log(site, f"Fejl ved skrivning til server: {e}")
        state.consecutive_errors += 1
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
            print(f"⏸️ {site}: backoff {backoff}s pga. gentagne server-write fejl")
            state.backoff_until = now + backoff
            state.consecutive_errors = 0
            close_modbus_reader(state.server_client)
            state.server_client = None
        return

    # Gem kun i database hvis der er gået DB_SAVE_INTERVAL sekunder
    should_save_to_db = (now - state.last_db_save >= DB_SAVE_INTERVAL)

    if should_save_to_db:
        state.last_import, state.last_export = save_data_to_db(
            dev,
            state.collection,
            state.last_import,
            state.last_export,
            ap,
            total_imp,
            total_exp,
            meta
        )
        state.last_db_save = now

    state.consecutive_errors = 0
    state.backoff_until = 0.0

    if state.last_run_at == 0.0:
        state.last_run_at = now
        state.next_run_at = now + SLEEP
    else:
        state.last_run_at = now
        state.next_run_at += SLEEP

    state.run_count += 1

    interval = actual_interval
    duration = time.time() - start_ts

    # Vis kun fejl og advarsler (ikke gentagne OK beskeder)
    should_print = False

    # Vis kun ved problemer eller periodisk (hvert 5. minut)
    if meta.get("active_power_invalid") or meta.get("import_invalid") or meta.get("export_invalid"):
        should_print = True
    elif interval > SLEEP * 2:
        should_print = True
    elif now - state.last_debug_print > 300:  # Hver 5. minut
        should_print = True

    if should_print:
        state.last_debug_print = now
        invalid_flags = []
        if meta.get("active_power_invalid"):
            invalid_flags.append("AP")
        if meta.get("import_invalid"):
            invalid_flags.append("IMP")
        if meta.get("export_invalid"):
            invalid_flags.append("EXP")
        invalid_str = "None" if not invalid_flags else ",".join(invalid_flags)

        status_icon = "⚠️" if invalid_flags else "✅"
        print(
            f"{status_icon} {site}: "
            f"AP={ap:.1f}W Imp={total_imp:.1f}Wh Exp={total_exp:.1f}Wh "
            f"(interval={interval:.1f}s, invalid={invalid_str})"
        )


# --------------------------------------------------------------------
# POLL EM-GROUP
# --------------------------------------------------------------------

def poll_emgroup_once(state: EmGroupState):
    start_ts = time.time()
    now = start_ts
    if now < state.backoff_until:
        return

    site = state.site
    srv = state.server

    if state.last_run_at > 0:
        actual_interval = now - state.last_run_at
    else:
        actual_interval = SLEEP

    if not state.server_client or not state.server_client.client.is_socket_open():
        ip = srv.get("ip", "localhost")
        port = srv.get("port", 650)
        tcp = connect_modbus_client(ip, port)
        if tcp:
            state.server_client = ModbusDataReader(tcp, 1)
            print(f"✅ {site}: server forbundet ({ip}:{port})")
        else:
            rate_limited_log(site, "kan ikke forbinde til server")
            state.consecutive_errors += 1
            if state.consecutive_errors >= 3:
                backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                print(f"⏸️ {site}: backoff {backoff}s pga. gentagne server-fejl (EM-group)")
                state.backoff_until = now + backoff
                state.consecutive_errors = 0
                close_modbus_reader(state.server_client)
                state.server_client = None
            return

    total_ap = 0.0
    total_imp_sum = 0.0
    total_exp_sum = 0.0
    success_count = 0

    group_meta = {
        "active_power_invalid": False,
        "active_power_raw": 0.0,
        "active_power_reason": "",
        "import_invalid": False,
        "import_raw": 0.0,
        "import_reason": "",
        "export_invalid": False,
        "export_raw": 0.0,
        "export_reason": "",
    }

    reasons_ap = []
    reasons_imp = []
    reasons_exp = []

    for em in state.ems:
        em_cfg = em["cfg"]
        em_site = em_cfg.get("site", "EM")
        if now < em["backoff_until"]:
            continue

        if not em["reader"] or not em["reader"].client.is_socket_open():
            ip = em_cfg["ip"]
            port = em_cfg["port"]
            tcp = connect_modbus_client(ip, port)
            if tcp:
                em["reader"] = ModbusDataReader(tcp, em_cfg["unit_id"])
                print(f"✅ {site}/{em_site}: device forbundet ({ip}:{port})")
            else:
                rate_limited_log(f"{site}/{em_site}", "kan ikke forbinde til EM-device")
                em["consecutive_errors"] += 1
                if em["consecutive_errors"] >= 3:
                    backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                    print(f"⏸️ {site}/{em_site}: backoff {backoff}s pga. gentagne device-fejl (EM)")
                    em["backoff_until"] = now + backoff
                    em["consecutive_errors"] = 0
                    close_modbus_reader(em["reader"])
                    em["reader"] = None
                continue

        try:
            ap, imp, exp, meta = read_energy_registers(
                em["reader"],
                em_cfg,
                0.0,
                0.0,
                actual_interval,
                False  # EM groups bruger ikke first_read
            )
        except ModbusException as e:
            rate_limited_log(f"{site}/{em_site}", f"Modbus fejl: {e}")
            em["consecutive_errors"] += 1
            close_modbus_reader(em["reader"])
            em["reader"] = None
            if em["consecutive_errors"] >= 3:
                backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                print(f"⏸️ {site}/{em_site}: backoff {backoff}s pga. gentagne Modbus-fejl (EM)")
                em["backoff_until"] = now + backoff
                em["consecutive_errors"] = 0
            continue
        except Exception as e:
            rate_limited_log(f"{site}/{em_site}", f"Uventet fejl i read: {e}")
            em["consecutive_errors"] += 1
            if em["consecutive_errors"] >= 3:
                backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
                print(f"⏸️ {site}/{em_site}: backoff {backoff}s pga. gentagne fejl (EM)")
                em["backoff_until"] = now + backoff
                em["consecutive_errors"] = 0
            continue

        em["consecutive_errors"] = 0
        total_ap += ap
        total_imp_sum += imp
        total_exp_sum += exp
        success_count += 1

        if meta.get("active_power_invalid"):
            group_meta["active_power_invalid"] = True
            reasons_ap.append(f"{em_site}: {meta.get('active_power_reason', '')}")
        group_meta["active_power_raw"] += meta.get("active_power_raw") or ap

        if meta.get("import_invalid"):
            group_meta["import_invalid"] = True
            reasons_imp.append(f"{em_site}: {meta.get('import_reason', '')}")
        group_meta["import_raw"] += meta.get("import_raw") or imp

        if meta.get("export_invalid"):
            group_meta["export_invalid"] = True
            reasons_exp.append(f"{em_site}: {meta.get('export_reason', '')}")
        group_meta["export_raw"] += meta.get("export_raw") or exp

    if success_count == 0:
        rate_limited_log(site, "Ingen succesfulde EM-læsninger i gruppe")
        state.consecutive_errors += 1
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
            print(f"⏸️ {site}: backoff {backoff}s pga. 0 succesfulde EM-læsninger")
            state.backoff_until = now + backoff
            state.consecutive_errors = 0
            close_modbus_reader(state.server_client)
            state.server_client = None
        return

    state.consecutive_errors = 0
    group_meta["active_power_reason"] = "; ".join(r for r in reasons_ap if r)
    group_meta["import_reason"] = "; ".join(r for r in reasons_imp if r)
    group_meta["export_reason"] = "; ".join(r for r in reasons_exp if r)

    try:
        safe_write(state.server_client, total_ap, "active_power", state.server_unit_id)
        if abs(total_imp_sum - state.last_import) > FLOAT_TOLERANCE_WH:
            safe_write(state.server_client, total_imp_sum, "total_import", state.server_unit_id)
        if abs(total_exp_sum - state.last_export) > FLOAT_TOLERANCE_WH:
            safe_write(state.server_client, total_exp_sum, "total_export", state.server_unit_id)
    except Exception as e:
        rate_limited_log(site, f"Fejl ved skrivning til server (EM-group): {e}")
        state.consecutive_errors += 1
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN, BACKOFF_MAX)
            print(f"⏸️ {site}: backoff {backoff}s pga. gentagne server-write fejl (EM-group)")
            state.backoff_until = now + backoff
            state.consecutive_errors = 0
            close_modbus_reader(state.server_client)
            state.server_client = None
        return

    group_device = {
        "site": state.group_name,
        "project_nr": state.ems[0]["cfg"].get("project_nr", ""),
        "name": state.group_name,
    }

    # Gem kun i database hvis der er gået DB_SAVE_INTERVAL sekunder
    should_save_to_db = (now - state.last_db_save >= DB_SAVE_INTERVAL)

    if should_save_to_db:
        state.last_import, state.last_export = save_data_to_db(
            group_device,
            state.collection,
            state.last_import,
            state.last_export,
            total_ap,
            total_imp_sum,
            total_exp_sum,
            group_meta
        )
        state.last_db_save = now

    state.backoff_until = 0.0
    if state.last_run_at == 0.0:
        state.last_run_at = now
        state.next_run_at = now + SLEEP
    else:
        state.last_run_at = now
        state.next_run_at += SLEEP

    state.run_count += 1

    interval = actual_interval if success_count > 0 else 0.0
    duration = time.time() - start_ts

    # Vis kun fejl og advarsler (ikke gentagne OK beskeder)
    should_print = False

    # Vis kun ved problemer eller periodisk (hvert 5. minut)
    if group_meta.get("active_power_invalid") or group_meta.get("import_invalid") or group_meta.get("export_invalid"):
        should_print = True
    elif interval > SLEEP * 2:
        should_print = True
    elif now - state.last_debug_print > 300:  # Hver 5. minut
        should_print = True

    if should_print:
        state.last_debug_print = now
        invalid_flags = []
        if group_meta.get("active_power_invalid"):
            invalid_flags.append("AP")
        if group_meta.get("import_invalid"):
            invalid_flags.append("IMP")
        if group_meta.get("export_invalid"):
            invalid_flags.append("EXP")
        invalid_str = "None" if not invalid_flags else ",".join(invalid_flags)

        status_icon = "⚠️" if invalid_flags else "✅"
        print(
            f"{status_icon} {state.group_name} (EM-group): "
            f"AP={total_ap:.1f}W Imp={total_imp_sum:.1f}Wh Exp={total_exp_sum:.1f}Wh "
            f"(interval={interval:.1f}s, invalid={invalid_str}, EMs={success_count})"
        )


# --------------------------------------------------------------------
# WORKER-TRÅD
# --------------------------------------------------------------------

class Worker(threading.Thread):
    def __init__(self, name: str, states: list):
        super().__init__(daemon=True, name=name)
        self.states = states

    def run(self):
        print(f"▶️ Worker {self.name} startet ({len(self.states)} states)")
        try:
            while not shutdown_event.is_set():
                now = time.time()
                with states_lock:
                    states_copy = list(self.states)
                for st in states_copy:
                    if now >= st.next_run_at and now >= st.backoff_until:
                        if getattr(st, "is_group", False):
                            poll_emgroup_once(st)
                        else:
                            poll_device_once(st)
                time.sleep(0.1)
        finally:
            print(f"⏹️ Worker {self.name} lukker connections...")
            with states_lock:
                for st in self.states:
                    if getattr(st, "is_group", False):
                        close_modbus_reader(st.server_client)
                        for em in st.ems:
                            close_modbus_reader(em["reader"])
                            em["reader"] = None
                    else:
                        close_modbus_reader(st.device_client)
                        close_modbus_reader(st.server_client)
                        st.device_client = None
                        st.server_client = None


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------

def reload_config_if_needed(cfg_loader, mongo_db, workers, last_reload_time, servers):
    """Reloader config fra MongoDB og opdaterer workers med nye/opdaterede devices."""
    now = time.time()
    if now - last_reload_time < CONFIG_RELOAD_INTERVAL:
        return last_reload_time

    print(f"\n🔄 Reloader config fra MongoDB...")
    try:
        cfg = cfg_loader.load_config()
        devices = cfg.get("devices", [])
        em_groups = cfg.get("em_groups", {})

        # Byg nye states
        new_states = []
        for dev in devices:
            for srv in servers:
                new_states.append(DeviceState(dev, srv, mongo_db))

        for group_name, group_devices in em_groups.items():
            for srv in servers:
                new_states.append(EmGroupState(group_name, group_devices, srv, mongo_db))

        # Byg set af nye state identifiers fra MongoDB
        new_state_ids = set()
        for new_st in new_states:
            if getattr(new_st, "is_group", False):
                new_state_ids.add(("group", new_st.group_name))
            else:
                new_state_ids.add(("device", new_st.site))

        with states_lock:
            # Find eksisterende state identifiers
            existing_ids = set()
            for w in workers:
                for st in w.states:
                    if getattr(st, "is_group", False):
                        existing_ids.add(("group", st.group_name))
                    else:
                        site = st.site
                        existing_ids.add(("device", site))

            # Fjern slettede devices/groups
            removed_count = 0
            for w in workers:
                original_count = len(w.states)
                w.states = [st for st in w.states
                           if (("group", st.group_name) if getattr(st, "is_group", False)
                               else ("device", st.site)) in new_state_ids]
                removed = original_count - len(w.states)
                removed_count += removed

            if removed_count > 0:
                print(f"🗑️ Fjernet {removed_count} slettede devices/groups")

            # Find nye states der ikke eksisterer
            new_count = 0
            for new_st in new_states:
                if getattr(new_st, "is_group", False):
                    state_id = ("group", new_st.group_name)
                else:
                    state_id = ("device", new_st.site)

                if state_id not in existing_ids:
                    # Tilføj til en passende worker (round-robin)
                    min_worker = min(workers, key=lambda w: len(w.states))
                    min_worker.states.append(new_st)
                    new_count += 1

            if new_count > 0:
                print(f"✅ Tilføjet {new_count} nye devices/groups")

            if new_count > 0 or removed_count > 0:
                total = sum(len(w.states) for w in workers)
                print(f"📊 Total states nu: {total}")
            else:
                print("ℹ️ Ingen ændringer i devices/groups")

        return now
    except Exception as e:
        print(f"[ERROR] Fejl ved config reload: {e}")
        return last_reload_time


def main():
    print("Starter Modbus Client Manager (Worker/DeviceState + EmGroupState)...")

    cfg_loader = ConfigLoader()
    try:
        cfg = cfg_loader.load_config()
    except Exception as e:
        print(f"[ERROR] Kan ikke indlæse config: {e}")
        return

    devices = cfg.get("devices", [])
    em_groups = cfg.get("em_groups", {})
    servers = cfg.get("servers", []) or list(SERVERS)
    print(f"Config: {len(devices)} devices, {len(em_groups)} EM-groups, {len(servers)} servers")

    mongo_client = MongoClient("mongodb://vbserver:27017", serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client[ENERGY_LOGS_DB]

    all_states: list = []

    for dev in devices:
        for srv in servers:
            all_states.append(DeviceState(dev, srv, mongo_db))

    for group_name, group_devices in em_groups.items():
        for srv in servers:
            all_states.append(EmGroupState(group_name, group_devices, srv, mongo_db))

    total_states = len(all_states)
    print(f"Total states (devices + EM-groups): {total_states}")

    if total_states == 0:
        print("Ingen states i config – afslutter")
        cfg_loader.close()
        mongo_client.close()
        return

    devices_per_worker = 2  # Færre devices per worker = mere parallelisme (optimeret til 200+ enheder)
    worker_count = max(1, min(200, (total_states + devices_per_worker - 1) // devices_per_worker))
    print(f"Opretter {worker_count} workers (ca. {devices_per_worker} states per)")
    print(f"🔄 Auto-reload aktiveret: Tjekker for nye devices hver {CONFIG_RELOAD_INTERVAL}s (5 min)\n")

    workers: list[Worker] = []
    for i in range(worker_count):
        chunk = all_states[i::worker_count]
        w = Worker(name=f"W{i+1}", states=chunk)
        w.start()
        workers.append(w)

    last_reload_time = time.time()

    try:
        while any(w.is_alive() for w in workers):
            time.sleep(5)
            last_reload_time = reload_config_if_needed(cfg_loader, mongo_db, workers, last_reload_time, servers)
    except KeyboardInterrupt:
        print("⏹️ Shutdown requested via KeyboardInterrupt")
        shutdown_event.set()
        time.sleep(2)

    cfg_loader.close()
    mongo_client.close()
    print("Program afsluttet.")


if __name__ == "__main__":
    main()