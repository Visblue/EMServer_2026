"""Modbus Client Manager (refactored)

This file is a refactored, documented version of `client_app.py`.

Goals
-----
- Keep the same overall behavior (poll Modbus devices, write to a Modbus "server",
  and periodically store measurements in MongoDB).
- Make the code easier to navigate and reason about by grouping responsibilities
  into small classes:

  - Config loading (MongoDB)
  - Modbus read/write (codec + reader)
  - Validation + business rules
  - Persistence (MongoDB repository)
  - Pollers (DevicePoller / EmGroupPoller)
  - Scheduling and worker threads

Notes
-----
- This file intentionally keeps most concepts and data fields from the original
  script so config documents and DB collections stay compatible.
- The original `client_app.py` is left unchanged.

Runtime dependencies
--------------------
- pymongo
- pymodbus (sync client)

"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import InvalidOperation
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusException
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder

import logging


# Silence noisy pymodbus logs; we explicitly log important failures ourselves.
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)


# --------------------------------------------------------------------
# Global constants / business rules
# --------------------------------------------------------------------

SLEEP_SECONDS_RANGE = (3, 5)  # seconds between polls per device (target interval)
DB_SAVE_INTERVAL_SECONDS = 30
CONFIG_RELOAD_INTERVAL_SECONDS = 120  # 5 minutes

MAX_REASONABLE_DIFF_WH = 500_000    # spikes above this are rejected
MAX_REASONABLE_POWER_W = 1_000_000  # 1 MW
FLOAT_TOLERANCE_WH = 0.01
MIN_TOTAL_VALUE = 0
MAX_TOTAL_RESET_THRESHOLD = 0.5  # meter reset if new < 50% of old

BACKOFF_MIN_SECONDS = 60
BACKOFF_MAX_SECONDS = 360

# Modbus register addresses used on the "server" side.
SERVER_REGISTERS = {
    "active_power": 19026,
    "total_import": 19068,
    "total_export": 19076,
}

# --------------------------------------------------------------------
# Infrastructure constants
# --------------------------------------------------------------------

# Source of truth for devices:
#   DB: Meters
#   Collection: devices
METERS_DB = "Meters"
METERS_DEVICES_COLLECTION = "devices"

# Energy logs DB (unchanged)
ENERGY_LOGS_DB = "customer_energy_logs"

# Servers are treated as constant (not loaded from MongoDB config)
SERVERS: list[dict[str, Any]] = [
    {"ip": "localhost", "port": 650},
]


# --------------------------------------------------------------------
# Error reporting (console + optional MongoDB)
# --------------------------------------------------------------------

@dataclass
class ErrorEvent:
    """Normalized representation of an error or warning we want to expose."""

    site: str
    message: str
    category: str = "runtime"  # e.g. connect/modbus/db/data/runtime
    severity: str = "error"    # error/warning/info


class ErrorReporter:
    """Interface used by pollers to record errors consistently."""

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        raise NotImplementedError


class RateLimitedConsoleReporter(ErrorReporter):
    """Prints a message at most once per `interval_seconds` per unique key.

    This prevents log spam while keeping a running counter.
    """

    def __init__(self) -> None:
        self._state: dict[str, dict[str, float | int]] = {}

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        key = f"{event.site}|{event.category}|{event.message}"
        now = time.time()

        st = self._state.setdefault(key, {"count": 0, "last_print": 0.0})
        st["count"] = int(st["count"]) + 1

        if now - float(st["last_print"]) >= interval_seconds:
            print(f"{event.site}: {event.message} (fejl #{int(st['count'])})")
            st["last_print"] = now


class ThrottledMongoEventReporter(ErrorReporter):
    """Persists aggregated events to MongoDB without spamming.

    Strategy
    --------
    - Use a stable primary key: (site, category, key)
    - Increment `count` and update `last_seen`.
    - Write at most once per interval per event key by buffering in memory.

    Collection schema (service_events)
    ---------------------------------
    {
      site: str,
      category: str,
      key: str,              # bounded text key (<= 240 chars)
      message: str,
      severity: str,
      first_seen: datetime,
      last_seen: datetime,
      count: int
    }
    """

    def __init__(self, collection: Collection) -> None:
        self._coll = collection
        self._state: dict[str, dict[str, float | int]] = {}

        # Ensure indexes exist (safe to call repeatedly).
        self._coll.create_index([("site", 1), ("category", 1), ("key", 1)], unique=True, background=True)
        self._coll.create_index([("last_seen", -1)], background=True)

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        key_text = event.message if len(event.message) <= 240 else event.message[:240]
        event_id = f"{event.site}|{event.category}|{key_text}"

        now = time.time()
        st = self._state.setdefault(event_id, {"pending": 0, "last_db": 0.0})
        st["pending"] = int(st["pending"]) + 1

        should_write = float(st["last_db"]) == 0.0 or (now - float(st["last_db"]) >= interval_seconds)
        if not should_write:
            return

        pending = int(st["pending"])
        if pending <= 0:
            return

        now_dt = datetime.utcnow()
        try:
            self._coll.update_one(
                {"site": event.site, "category": event.category, "key": key_text},
                {
                    "$setOnInsert": {"first_seen": now_dt, "count": 0},
                    "$set": {"last_seen": now_dt, "message": event.message, "severity": event.severity},
                    "$inc": {"count": pending},
                },
                upsert=True,
            )
            st["pending"] = 0
            st["last_db"] = now
        except Exception:
            # Monitoring must never break polling.
            pass


class CompositeReporter(ErrorReporter):
    """Fan-out reporter that forwards the same event to multiple reporters."""

    def __init__(self, reporters: list[ErrorReporter]) -> None:
        self._reporters = reporters

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        for rep in self._reporters:
            rep.record(event, interval_seconds=interval_seconds)


def categorize_message(message: str) -> tuple[str, str]:
    """Infer (category, severity) from a free-text message."""

    msg_l = message.lower()
    if "invalid" in msg_l:
        return "data", "warning"
    if "mongodb" in msg_l:
        return "db", "error"
    if "modbus" in msg_l:
        return "modbus", "error"
    if "forbinde" in msg_l or "connect" in msg_l:
        return "connect", "error"
    return "runtime", "error"


# --------------------------------------------------------------------
# Modbus codec + reader
# --------------------------------------------------------------------

class ModbusCodec:
    """Collection of Modbus register decoding helpers."""

    @staticmethod
    def _decoder(registers: list[int], byteorder: Any, wordorder: Any) -> BinaryPayloadDecoder:
        return BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)

    def decode_int16(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_16bit_int()

    def decode_uint16(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_16bit_uint()

    def decode_uint32(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_uint()

    def decode_int32_big_little(self, registers: list[int]) -> int:
        # Matches original `decode_int32` (byteorder Big, wordorder Little).
        return self._decoder(registers, Endian.Big, Endian.Little).decode_32bit_int()

    def decode_int32_big_big(self, registers: list[int]) -> int:
        # Matches original `decode_int32_1`.
        return self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_int()

    def decode_float32_big_big(self, registers: list[int]) -> float:
        return round(self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_float(), 3)

    def decode_float32_big_little(self, registers: list[int]) -> float:
        return round(self._decoder(registers, Endian.Big, Endian.Little).decode_32bit_float(), 3)

    def decode_float64_big_big(self, registers: list[int]) -> float:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_64bit_float()

    def decode_float64_big_little(self, registers: list[int]) -> float:
        return self._decoder(registers, Endian.Big, Endian.Little).decode_64bit_float()


class ModbusReader(ModbusCodec):
    """Thin wrapper around `ModbusTcpClient` with read/write helpers.

        The config for a register is expected to contain:
        - address: int
        - format: str (int16/uint16/uint32/int32/int32_1/float32/float32_1/float32_2/double/double_little)
        - reading/table: optional str ("holding" or "input"). If omitted, the caller can
            provide a default (typically device_cfg.get("reading", "holding")).
        - count: optional int (ignored for most known formats)
    """

    def __init__(self, client: ModbusTcpClient, unit_id: int):
        self.client = client
        self.unit_id = unit_id

    def is_open(self) -> bool:
        """True when the underlying socket is open."""
        try:
            return bool(self.client.is_socket_open())
        except Exception:
            return False

    def close(self) -> None:
        """Close the underlying TCP connection."""
        try:
            self.client.close()
        except Exception:
            pass

    def read_register(self, reg_cfg: dict[str, Any], *, default_table: str = "holding") -> float | int:
        """Read and decode either a holding or input register.

        Selection rules:
        - reg_cfg["reading"] or reg_cfg["table"] can be set to "input" or "holding".
        - If absent, `default_table` is used (typically device_cfg.get("reading", "holding")).
        """

        addr = int(reg_cfg["address"])
        fmt = reg_cfg["format"]

        table = (reg_cfg.get("reading") or reg_cfg.get("table") or default_table or "holding").strip().lower()
        if table not in ("holding", "input"):
            raise ValueError(f"Ukendt register-table: {table} (for {addr})")

        read_fn = self.client.read_input_registers if table == "input" else self.client.read_holding_registers

        if fmt in ("int16", "uint16"):
            res = read_fn(addr, count=1, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int16(res.registers) if fmt == "int16" else self.decode_uint16(res.registers)

        if fmt == "uint32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if len(res.registers) != 2:
                raise ModbusException(f"uint32 expected 2 regs, got {len(res.registers)}")
            return self.decode_uint32(res.registers)

        if fmt == "int32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32_big_little(res.registers)

        if fmt == "int32_1":
            # Keep compatibility with the original format naming: int32_1 means Big/Big decoding.
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32_big_big(res.registers)

        if fmt in ("float32", "float32_1", "float32_2"):
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if fmt in ("float32", "float32_1"):
                return self.decode_float32_big_big(res.registers)
            return self.decode_float32_big_little(res.registers)

        if fmt == "double":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float64_big_big(res.registers)

        if fmt == "double_little":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float64_big_little(res.registers)

        raise ValueError(f"Ukendt format: {fmt}")

    def read_register_holding(self, reg_cfg: dict[str, Any]) -> float | int:
        """Backward-compatible wrapper (forces holding registers)."""

        return self.read_register(reg_cfg, default_table="holding")

    def read_input_register(self, reg_cfg: dict[str, Any]) -> float | int:
        """Backward-compatible wrapper (forces input registers)."""

        return self.read_register(reg_cfg, default_table="input")

    def write_float32(self, value: float, address: int, unit_id: int) -> None:
        """Write a 32-bit float to the given address."""

        builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
        builder.add_32bit_float(float(value))
        regs = builder.to_registers()

        res = self.client.write_registers(address, regs, unit=unit_id)
        if res.isError():
            raise ModbusException(f"Write error at {address}")


def connect_modbus(ip: str, port: int, timeout_seconds: int = 1) -> Optional[ModbusTcpClient]:
    """Connect to a Modbus TCP endpoint and return the client (or None)."""

    try:
        client = ModbusTcpClient(ip, port=port, timeout=timeout_seconds, retries=1, retry_on_empty=True)
        if client.connect():
            return client
        client.close()
    except Exception:
        pass
    return None


# --------------------------------------------------------------------
# MongoDB config loading
# --------------------------------------------------------------------

class MongoConfigLoader:
    """Loads devices from MongoDB: `Meters.devices` (one document per device)."""

    def __init__(self, client: MongoClient | None = None, uri: str = "mongodb://vbserver:27017") -> None:
        self._owns_client = client is None
        self._client = client or MongoClient(uri, serverSelectionTimeoutMS=5000)
        self._db = self._client[METERS_DB]
        self._coll = self._db[METERS_DEVICES_COLLECTION]

    def load(self) -> dict[str, Any]:
        """Return config dict and expand em_groups structure."""

        devices = list(self._coll.find({}, {"_id": 0}))
        if not devices:
            raise ValueError("Ingen devices fundet i MongoDB (Meters.devices)")

        cfg: dict[str, Any] = {"devices": devices, "servers": list(SERVERS)}
        em_groups: dict[str, list[dict[str, Any]]] = {}
        single_devices: list[dict[str, Any]] = []

        for dev in devices:
            group = dev.get("em_group")
            if group:
                em_groups.setdefault(group, []).append(dev)
            else:
                single_devices.append(dev)

        if em_groups:
            print(f"[INFO] Fundet {len(em_groups)} em_groups i config.")

        cfg["devices"] = single_devices
        cfg["em_groups"] = em_groups
        return cfg

    def close(self) -> None:
        """Close the client only if we own it (created internally)."""
        if self._owns_client:
            self._client.close()


# --------------------------------------------------------------------
# Validation + energy reading
# --------------------------------------------------------------------

@dataclass
class EnergyMeta:
    """Meta flags used by frontend and DB to show invalid readings."""

    active_power_invalid: bool = False
    active_power_raw: Optional[float] = None
    active_power_reason: str = ""

    import_invalid: bool = False
    import_raw: Optional[float] = None
    import_reason: str = ""

    export_invalid: bool = False
    export_raw: Optional[float] = None
    export_reason: str = ""

    @property
    def has_invalid_data(self) -> bool:
        return bool(self.active_power_invalid or self.import_invalid or self.export_invalid)


class EnergyValidator:
    """Business rules for validating totals."""

    def validate_total(self, new: float, last: float, value_name: str, *, first_read: bool) -> tuple[bool, float, str]:
        """Return (ok, validated_value, reason)."""

        if new < MIN_TOTAL_VALUE:
            return False, last, f"{value_name} negativ ({new})"

        if first_read:
            return True, new, "første læsning efter restart"

        if last <= 1.0:
            return True, new, "første læsning"

        # Detect meter reset.
        if new < last * MAX_TOTAL_RESET_THRESHOLD:
            return True, new, "meter reset"

        diff = new - last

        if diff < -FLOAT_TOLERANCE_WH:
            return False, last, f"{value_name} faldt ({last} -> {new})"

        # Early phase tolerance.
        if last < 10000 and diff > MAX_REASONABLE_DIFF_WH:
            extended_limit = MAX_REASONABLE_DIFF_WH * 10
            if diff > extended_limit:
                return False, last, f"{value_name} spike ({diff} Wh) selv med udvidet grænse"
            return True, new, f"OK (tidlig fase, diff={diff} Wh)"

        if diff > MAX_REASONABLE_DIFF_WH:
            return False, last, f"{value_name} spike ({diff} Wh)"

        return True, new, "OK"


class EnergyReader:
    """Reads active power and totals from a ModbusReader according to config."""

    def __init__(self, validator: EnergyValidator, reporter: ErrorReporter) -> None:
        self._validator = validator
        self._reporter = reporter

    def read(self,
             reader: ModbusReader,
             device_cfg: dict[str, Any],
             last_import: float,
             last_export: float,
             interval_seconds: float,
             *,
             first_read: bool) -> tuple[float, float, float, EnergyMeta]:
        """Return (active_power, total_import, total_export, meta)."""

        regs = device_cfg["registers"]
        site = device_cfg.get("site", "unknown")

        has_ap = "active_power" in regs
        has_imp = "total_import" in regs
        has_exp = "total_export" in regs

        active_power = 0.0
        total_import = last_import
        total_export = last_export

        meta = EnergyMeta()

        # Device-level default: can be overridden per register via regs[...]["reading"] or ["table"].
        default_table = (device_cfg.get("reading") or device_cfg.get("table") or "holding").strip().lower()

        def read_reg(reg_cfg: dict[str, Any]) -> float | int:
            return reader.read_register(reg_cfg, default_table=default_table)

        if has_ap:
            raw = read_reg(regs["active_power"])
            scale = regs["active_power"].get("scale", 1.0)
            ap_raw = float(raw) * float(scale)

            # Offsets are applied AFTER scaling.
            # Use registers.active_power.offset
            ap_offset = regs["active_power"].get("offset", 0.0) or 0.0
            ap_raw = ap_raw + float(ap_offset)

            meta.active_power_raw = ap_raw

            if ap_raw != ap_raw or abs(ap_raw) > MAX_REASONABLE_POWER_W:
                meta.active_power_invalid = True
                meta.active_power_reason = f"Urealistisk active_power: {ap_raw}"
                cat, sev = categorize_message(f"Active_power invalid: {ap_raw}")
                self._reporter.record(
                    ErrorEvent(site=site, message=f"Active_power invalid: {ap_raw}", category=cat, severity=sev)
                )
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
            meta.import_raw = new_val

            ok, validated, reason = self._validator.validate_total(new_val, last_import, "Import", first_read=first_read)
            if not ok:
                meta.import_invalid = True
                meta.import_reason = reason
                cat, sev = categorize_message(f"Import invalid: {reason}")
                self._reporter.record(
                    ErrorEvent(site=site, message=f"Import invalid: {reason}", category=cat, severity=sev)
                )
            total_import = validated

        if has_exp:
            raw = read_reg(regs["total_export"])
            scale = regs["total_export"].get("scale", 1.0)
            new_val = round(float(raw) * float(scale), 3)
            exp_offset = regs["total_export"].get("offset")
            if exp_offset is not None:
                new_val = round(new_val + float(exp_offset), 3)
            meta.export_raw = new_val

            ok, validated, reason = self._validator.validate_total(new_val, last_export, "Export", first_read=first_read)
            if not ok:
                meta.export_invalid = True
                meta.export_reason = reason
                cat, sev = categorize_message(f"Export invalid: {reason}")
                self._reporter.record(
                    ErrorEvent(site=site, message=f"Export invalid: {reason}", category=cat, severity=sev)
                )
            total_export = validated

        # If only AP is available, derive totals by integrating power over time.
        if has_ap and not (has_imp or has_exp):
            effective_interval = max(float(interval_seconds), 1.0)
            energy_wh = (active_power * effective_interval) / 3600.0
            if active_power > 0:
                total_import = last_import + energy_wh
            elif active_power < 0:
                total_export = last_export + abs(energy_wh)

        return active_power, total_import, total_export, meta


# --------------------------------------------------------------------
# MongoDB repository (load last totals + save data)
# --------------------------------------------------------------------

class MongoEnergyRepository:
    """Handles MongoDB persistence for energy samples."""

    def __init__(self, mongo_db: Database, reporter: ErrorReporter) -> None:
        self._db = mongo_db
        self._reporter = reporter
        self._indexed_collections: set[str] = set()  # Cache to avoid repeated index creation

    @staticmethod
    def collection_name(device_cfg: dict[str, Any]) -> str:
        """Resolve the collection name from the device config."""

        if "data_collection" in device_cfg:
            return device_cfg["data_collection"]
        project = device_cfg.get("project_nr", "")
        name = device_cfg.get("name", "device")
        return f"{project}_{name}" if project else name

    def get_collection(self, device_cfg: dict[str, Any]) -> Collection:
        """Return the collection for the device and ensure an index (cached)."""

        coll_name = self.collection_name(device_cfg)
        coll = self._db[coll_name]
        # Only create index once per collection name
        if coll_name not in self._indexed_collections:
            coll.create_index([("Time", -1)], background=True)
            self._indexed_collections.add(coll_name)
        return coll

    def load_last_totals(self, collection: Collection) -> tuple[float, float]:
        """Load last totals from a collection, or (0.0, 0.0)."""

        try:
            latest = collection.find_one(
                {},
                {"_id": 0, "Total_Imported": 1, "Total_Exported": 1},
                sort=[("Time", -1)],
            )
            if latest:
                imp = float(latest.get("Total_Imported", 0.0))
                exp = float(latest.get("Total_Exported", 0.0))
                return imp, exp
        except Exception as e:
            self._reporter.record(ErrorEvent(site=collection.name, message=f"Fejl ved indlæsning af sidste totals: {e}", category="db"))
        return 0.0, 0.0

    def save_sample(self,
                    device_cfg: dict[str, Any],
                    collection: Collection,
                    last_import: float,
                    last_export: float,
                    active_power: float,
                    total_import: float,
                    total_export: float,
                    meta: EnergyMeta) -> tuple[float, float]:
        """Persist a sample and return updated (last_import, last_export)."""

        site = device_cfg.get("site", "unknown")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Guard against NaNs.
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
            "Project_nr": device_cfg.get("project_nr", ""),
            "Device_name": device_cfg.get("name", ""),
            "Time": now,
            "Active_power": round(active_power, 2),
            "Imported_Wh": imp_diff,
            "Exported_Wh": exp_diff,
            "Total_Imported": total_import,
            "Total_Exported": total_export,
            "Total_Imported_kWh": total_import / 1000,
            "Total_Exported_kWh": total_export / 1000,
            "Active_power_valid": not meta.active_power_invalid,
            "Active_power_raw": meta.active_power_raw,
            "Active_power_invalid_reason": meta.active_power_reason,
            "Import_valid": not meta.import_invalid,
            "Import_raw": meta.import_raw,
            "Import_invalid_reason": meta.import_reason,
            "Export_valid": not meta.export_invalid,
            "Export_raw": meta.export_raw,
            "Export_invalid_reason": meta.export_reason,
            "Has_invalid_data": meta.has_invalid_data,
        }

        try:
            collection.insert_one(doc)
        except InvalidOperation:
            print(f"💥 {site}: Mongo client lukket – kan ikke gemme")
            return last_import, last_export
        except Exception as e:
            cat, sev = categorize_message(f"MongoDB fejl: {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"MongoDB fejl: {e}", category=cat, severity=sev))
            return last_import, last_export

        return total_import, total_export


# --------------------------------------------------------------------
# State models
# --------------------------------------------------------------------

@dataclass
class DeviceState:
    """State for polling a single device and writing to a server."""

    device_cfg: dict[str, Any]
    server_cfg: dict[str, Any]
    collection: Collection
    last_import: float
    last_export: float

    site: str
    server_unit_id: int

    device_reader: Optional[ModbusReader] = None
    server_reader: Optional[ModbusReader] = None

    first_read: bool = True

    consecutive_errors: int = 0
    backoff_until: float = 0.0

    next_run_at: float = field(default_factory=lambda: time.time() + random.uniform(*SLEEP_SECONDS_RANGE))
    last_run_at: float = 0.0

    last_db_save: float = 0.0
    run_count: int = 0
    last_debug_print: float = 0.0


@dataclass
class EmMemberState:
    """State for a single EM inside an EM-group."""

    cfg: dict[str, Any]
    reader: Optional[ModbusReader] = None
    consecutive_errors: int = 0
    backoff_until: float = 0.0


@dataclass
class EmGroupState:
    """State for polling multiple EM devices and writing an aggregate to a server."""

    group_name: str
    members: list[EmMemberState]
    server_cfg: dict[str, Any]
    collection: Collection

    last_import: float
    last_export: float

    site: str
    server_unit_id: int

    server_reader: Optional[ModbusReader] = None

    consecutive_errors: int = 0
    backoff_until: float = 0.0

    next_run_at: float = field(default_factory=lambda: time.time() + random.uniform(*SLEEP_SECONDS_RANGE))
    last_run_at: float = 0.0

    last_db_save: float = 0.0
    run_count: int = 0
    last_debug_print: float = 0.0


# --------------------------------------------------------------------
# Polling helpers
# --------------------------------------------------------------------

def safe_write_float32(reader: ModbusReader, value: Any, reg_key: str, unit_id: int) -> None:
    """Write a float32 to the server, ignoring None/NaN/inf/invalid types."""

    if value is None:
        return

    try:
        if value != value:
            return
        if abs(value) == float("inf"):
            return
        if not isinstance(value, (int, float)):
            return
        reader.write_float32(float(value), SERVER_REGISTERS[reg_key], unit_id)
    except Exception as e:
        print(f"[ERROR] Fejl ved skrivning til {reg_key}: {e}")


class DevicePoller:
    """Encapsulates the poll loop for a single device state."""

    def __init__(self, energy_reader: EnergyReader, repo: MongoEnergyRepository, reporter: ErrorReporter) -> None:
        self._energy_reader = energy_reader
        self._repo = repo
        self._reporter = reporter

    def poll_once(self, state: DeviceState) -> None:
        now = time.time()
        if now < state.backoff_until:
            return

        site = state.site
        dev = state.device_cfg
        srv = state.server_cfg

        actual_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)

        # Ensure device connection.
        if not state.device_reader or not state.device_reader.is_open():
            tcp = connect_modbus(dev["ip"], dev["port"])
            if tcp:
                state.device_reader = ModbusReader(tcp, dev["unit_id"])
                print(f"✅ {site}: device forbundet ({dev['ip']}:{dev['port']})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til device")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til device", category=cat, severity=sev))
                self._apply_error_backoff(state, close_device=True)
                return

        # Ensure server connection.
        if not state.server_reader or not state.server_reader.is_open():
            ip = srv.get("ip", "localhost")
            port = srv.get("port", 650)
            tcp = connect_modbus(ip, port)
            if tcp:
                state.server_reader = ModbusReader(tcp, 1)
                print(f"✅ {site}: server forbundet ({ip}:{port})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til server")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til server", category=cat, severity=sev))
                self._apply_error_backoff(state, close_server=True)
                return

        # Read values.
        try:
            ap, total_imp, total_exp, meta = self._energy_reader.read(
                state.device_reader,
                dev,
                state.last_import,
                state.last_export,
                actual_interval,
                first_read=state.first_read,
            )
            state.first_read = False
        except ModbusException as e:
            cat, sev = categorize_message(f"Modbus fejl: {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Modbus fejl: {e}", category=cat, severity=sev))
            state.consecutive_errors += 1
            if state.device_reader:
                state.device_reader.close()
            state.device_reader = None
            self._apply_error_backoff(state)
            return
        except Exception as e:
            cat, sev = categorize_message(f"Uventet fejl i read: {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Uventet fejl i read: {e}", category=cat, severity=sev))
            state.consecutive_errors += 1
            self._apply_error_backoff(state)
            return

        # Write values to server.
        try:
            assert state.server_reader is not None
            safe_write_float32(state.server_reader, ap, "active_power", state.server_unit_id)
            if abs(total_imp - state.last_import) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_imp, "total_import", state.server_unit_id)
            if abs(total_exp - state.last_export) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_exp, "total_export", state.server_unit_id)
        except Exception as e:
            cat, sev = categorize_message(f"Fejl ved skrivning til server: {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Fejl ved skrivning til server: {e}", category=cat, severity=sev))
            state.consecutive_errors += 1
            self._apply_error_backoff(state, close_server=True)
            return

        # Persist to DB periodically.
        if now - state.last_db_save >= DB_SAVE_INTERVAL_SECONDS:
            state.last_import, state.last_export = self._repo.save_sample(
                dev,
                state.collection,
                state.last_import,
                state.last_export,
                ap,
                total_imp,
                total_exp,
                meta,
            )
            state.last_db_save = now

        # Success path.
        state.consecutive_errors = 0
        state.backoff_until = 0.0

        state.last_run_at = now
        state.next_run_at = (state.next_run_at + random.randint(*SLEEP_SECONDS_RANGE)) if state.run_count > 0 else now + random.randint(*SLEEP_SECONDS_RANGE)
        state.run_count += 1

        self._maybe_print_status(state, ap, total_imp, total_exp, meta, actual_interval)

    def _apply_error_backoff(self, state: DeviceState, *, close_device: bool = False, close_server: bool = False) -> None:
        """Apply backoff after repeated errors."""

        state.consecutive_errors += 1

        if close_device and state.device_reader:
            state.device_reader.close()
            state.device_reader = None
        if close_server and state.server_reader:
            state.server_reader.close()
            state.server_reader = None

        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {state.site}: backoff {backoff}s pga. gentagne fejl")
            state.backoff_until = time.time() + backoff
            state.consecutive_errors = 0

    def _maybe_print_status(self,
                           state: DeviceState,
                           ap: float,
                           total_imp: float,
                           total_exp: float,
                           meta: EnergyMeta,
                           interval_seconds: float) -> None:
        """Print status periodically or when there are invalid readings."""

        now = time.time()
        should_print = False

        if meta.has_invalid_data:
            should_print = True
        elif interval_seconds > max(SLEEP_SECONDS_RANGE) * 2:
            should_print = True
        elif now - state.last_debug_print > 300:
            should_print = True

        if not should_print:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")
        invalid_str = "None" if not invalid_flags else ",".join(invalid_flags)

        status_icon = "⚠️" if invalid_flags else "✅"
        print(
            f"{status_icon} {state.site}: "
            f"AP={ap:.1f}W Imp={total_imp:.1f}Wh Exp={total_exp:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={invalid_str})"
        )


class EmGroupPoller:
    """Poller for an EM-group (sum of multiple EM devices -> one server write)."""

    def __init__(self, energy_reader: EnergyReader, repo: MongoEnergyRepository, reporter: ErrorReporter) -> None:
        self._energy_reader = energy_reader
        self._repo = repo
        self._reporter = reporter

    def poll_once(self, state: EmGroupState) -> None:
        now = time.time()
        if now < state.backoff_until:
            return

        site = state.site
        srv = state.server_cfg
        actual_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)

        # Ensure server connection.
        if not state.server_reader or not state.server_reader.is_open():
            ip = srv.get("ip", "localhost")
            port = srv.get("port", 650)
            tcp = connect_modbus(ip, port)
            if tcp:
                state.server_reader = ModbusReader(tcp, 1)
                print(f"✅ {site}: server forbundet ({ip}:{port})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til server")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til server", category=cat, severity=sev))
                state.consecutive_errors += 1
                self._apply_group_backoff(state, close_server=True)
                return

        total_ap = 0.0
        total_imp_sum = 0.0
        total_exp_sum = 0.0
        success_count = 0

        # Aggregate meta.
        group_meta = EnergyMeta(
            active_power_raw=0.0,
            import_raw=0.0,
            export_raw=0.0,
        )
        reasons_ap: list[str] = []
        reasons_imp: list[str] = []
        reasons_exp: list[str] = []

        for member in state.members:
            em_cfg = member.cfg
            em_site = em_cfg.get("site", "EM")

            if now < member.backoff_until:
                continue

            if not member.reader or not member.reader.is_open():
                tcp = connect_modbus(em_cfg["ip"], em_cfg["port"])
                if tcp:
                    member.reader = ModbusReader(tcp, em_cfg["unit_id"])
                    print(f"✅ {site}/{em_site}: device forbundet ({em_cfg['ip']}:{em_cfg['port']})")
                else:
                    cat, sev = categorize_message("kan ikke forbinde til EM-device")
                    self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message="kan ikke forbinde til EM-device", category=cat, severity=sev))
                    self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                    continue

            try:
                ap, imp, exp, meta = self._energy_reader.read(
                    member.reader,
                    em_cfg,
                    0.0,
                    0.0,
                    actual_interval,
                    first_read=False,
                )
            except ModbusException as e:
                cat, sev = categorize_message(f"Modbus fejl: {e}")
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=f"Modbus fejl: {e}", category=cat, severity=sev))
                if member.reader:
                    member.reader.close()
                member.reader = None
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue
            except Exception as e:
                cat, sev = categorize_message(f"Uventet fejl i read: {e}")
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=f"Uventet fejl i read: {e}", category=cat, severity=sev))
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue

            member.consecutive_errors = 0

            total_ap += ap
            total_imp_sum += imp
            total_exp_sum += exp
            success_count += 1

            if meta.active_power_invalid:
                group_meta.active_power_invalid = True
                reasons_ap.append(f"{em_site}: {meta.active_power_reason}")
            group_meta.active_power_raw = float(group_meta.active_power_raw or 0.0) + float(meta.active_power_raw or ap)

            if meta.import_invalid:
                group_meta.import_invalid = True
                reasons_imp.append(f"{em_site}: {meta.import_reason}")
            group_meta.import_raw = float(group_meta.import_raw or 0.0) + float(meta.import_raw or imp)

            if meta.export_invalid:
                group_meta.export_invalid = True
                reasons_exp.append(f"{em_site}: {meta.export_reason}")
            group_meta.export_raw = float(group_meta.export_raw or 0.0) + float(meta.export_raw or exp)

        if success_count == 0:
            cat, sev = categorize_message("Ingen succesfulde EM-læsninger i gruppe")
            self._reporter.record(ErrorEvent(site=site, message="Ingen succesfulde EM-læsninger i gruppe", category=cat, severity=sev))
            state.consecutive_errors += 1
            self._apply_group_backoff(state, close_server=True)
            return

        state.consecutive_errors = 0
        group_meta.active_power_reason = "; ".join(r for r in reasons_ap if r)
        group_meta.import_reason = "; ".join(r for r in reasons_imp if r)
        group_meta.export_reason = "; ".join(r for r in reasons_exp if r)

        # Write aggregated values.
        try:
            assert state.server_reader is not None
            safe_write_float32(state.server_reader, total_ap, "active_power", state.server_unit_id)
            if abs(total_imp_sum - state.last_import) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_imp_sum, "total_import", state.server_unit_id)
            if abs(total_exp_sum - state.last_export) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_exp_sum, "total_export", state.server_unit_id)
        except Exception as e:
            cat, sev = categorize_message(f"Fejl ved skrivning til server (EM-group): {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Fejl ved skrivning til server (EM-group): {e}", category=cat, severity=sev))
            state.consecutive_errors += 1
            self._apply_group_backoff(state, close_server=True)
            return

        # Persist to DB periodically.
        if now - state.last_db_save >= DB_SAVE_INTERVAL_SECONDS:
            group_device_cfg = {
                "site": state.group_name,
                "project_nr": state.members[0].cfg.get("project_nr", "") if state.members else "",
                "name": state.group_name,
            }
            state.last_import, state.last_export = self._repo.save_sample(
                group_device_cfg,
                state.collection,
                state.last_import,
                state.last_export,
                total_ap,
                total_imp_sum,
                total_exp_sum,
                group_meta,
            )
            state.last_db_save = now

        # Success path scheduling.
        state.backoff_until = 0.0
        state.last_run_at = now
        state.next_run_at = (state.next_run_at + random.randint(*SLEEP_SECONDS_RANGE)) if state.run_count > 0 else now + random.randint(*SLEEP_SECONDS_RANGE)
        state.run_count += 1

        self._maybe_print_status(state, total_ap, total_imp_sum, total_exp_sum, group_meta, actual_interval, success_count)

    def _apply_member_backoff(self, member: EmMemberState, *, prefix: str) -> None:
        member.consecutive_errors += 1
        if member.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {prefix}: backoff {backoff}s pga. gentagne fejl (EM)")
            member.backoff_until = time.time() + backoff
            member.consecutive_errors = 0

    def _apply_group_backoff(self, state: EmGroupState, *, close_server: bool = False) -> None:
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {state.site}: backoff {backoff}s pga. gentagne fejl (EM-group)")
            state.backoff_until = time.time() + backoff
            state.consecutive_errors = 0
            if close_server and state.server_reader:
                state.server_reader.close()
                state.server_reader = None

    def _maybe_print_status(self,
                           state: EmGroupState,
                           total_ap: float,
                           total_imp_sum: float,
                           total_exp_sum: float,
                           meta: EnergyMeta,
                           interval_seconds: float,
                           success_count: int) -> None:
        now = time.time()
        should_print = False

        if meta.has_invalid_data:
            should_print = True
        elif interval_seconds > max(SLEEP_SECONDS_RANGE) * 2:
            should_print = True
        elif now - state.last_debug_print > 300:
            should_print = True

        if not should_print:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")
        invalid_str = "None" if not invalid_flags else ",".join(invalid_flags)

        status_icon = "⚠️" if invalid_flags else "✅"
        print(
            f"{status_icon} {state.group_name} (EM-group): "
            f"AP={total_ap:.1f}W Imp={total_imp_sum:.1f}Wh Exp={total_exp_sum:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={invalid_str}, EMs={success_count})"
        )


# --------------------------------------------------------------------
# Worker scheduling
# --------------------------------------------------------------------

shutdown_event = threading.Event()
states_lock = threading.Lock()


class Worker(threading.Thread):
    """Thread that polls its assigned states when they are due."""

    def __init__(self, name: str, device_states: list[DeviceState], group_states: list[EmGroupState],
                 device_poller: DevicePoller, group_poller: EmGroupPoller):
        super().__init__(daemon=True, name=name)
        self.device_states = device_states
        self.group_states = group_states
        self.device_poller = device_poller
        self.group_poller = group_poller

    def run(self) -> None:
        print(f"▶️ Worker {self.name} startet ({len(self.device_states) + len(self.group_states)} states)")
        try:
            while not shutdown_event.is_set():
                now = time.time()
                with states_lock:
                    devices_copy = list(self.device_states)
                    groups_copy = list(self.group_states)

                for st in devices_copy:
                    if now >= st.next_run_at and now >= st.backoff_until:
                        self.device_poller.poll_once(st)

                for st in groups_copy:
                    if now >= st.next_run_at and now >= st.backoff_until:
                        self.group_poller.poll_once(st)

                time.sleep(0.2)
        finally:
            print(f"⏹️ Worker {self.name} lukker connections...")
            with states_lock:
                for st in self.device_states:
                    if st.device_reader:
                        st.device_reader.close()
                    if st.server_reader:
                        st.server_reader.close()
                    st.device_reader = None
                    st.server_reader = None

                for st in self.group_states:
                    if st.server_reader:
                        st.server_reader.close()
                    st.server_reader = None
                    for m in st.members:
                        if m.reader:
                            m.reader.close()
                        m.reader = None


# --------------------------------------------------------------------
# State builder + config reloader
# --------------------------------------------------------------------

class StateFactory:
    """Creates state objects (DeviceState / EmGroupState) from config."""

    def __init__(self, repo: MongoEnergyRepository) -> None:
        self._repo = repo

    def build_device_states(self, devices: list[dict[str, Any]], servers: list[dict[str, Any]]) -> list[DeviceState]:
        out: list[DeviceState] = []
        for dev in devices:
            coll = self._repo.get_collection(dev)
            last_imp, last_exp = self._repo.load_last_totals(coll)
            for srv in servers:
                out.append(
                    DeviceState(
                        device_cfg=dev,
                        server_cfg=srv,
                        collection=coll,
                        last_import=last_imp,
                        last_export=last_exp,
                        site=dev.get("site", "unknown"),
                        server_unit_id=dev.get("server_unit_id", 1),
                    )
                )
        return out

    def build_group_states(self, em_groups: dict[str, list[dict[str, Any]]], servers: list[dict[str, Any]]) -> list[EmGroupState]:
        out: list[EmGroupState] = []
        for group_name, group_devices in em_groups.items():
            if not group_devices:
                continue

            # Use the first member to decide collection naming (compatible with original behavior).
            first = group_devices[0]
            group_device_cfg = {
                "site": group_name,
                "project_nr": first.get("project_nr", ""),
                "name": group_name,
                "data_collection": first.get("data_collection"),
            }
            coll = self._repo.get_collection(group_device_cfg)
            last_imp, last_exp = self._repo.load_last_totals(coll)

            members = [EmMemberState(cfg=d) for d in group_devices]

            for srv in servers:
                out.append(
                    EmGroupState(
                        group_name=group_name,
                        members=members,
                        server_cfg=srv,
                        collection=coll,
                        last_import=last_imp,
                        last_export=last_exp,
                        site=f"EM_GROUP:{group_name}",
                        server_unit_id=group_devices[0].get("server_unit_id", 1),
                    )
                )
        return out


def reload_config_if_needed(
    cfg_loader: MongoConfigLoader,
    repo: MongoEnergyRepository,
    factory: StateFactory,
    workers: list[Worker],
    last_reload_time: float,
    servers: list[dict[str, Any]],
) -> float:
    """Reload config periodically and update worker state lists.

    This is a best-effort reload, designed to avoid disrupting existing threads.
    """

    now = time.time()
    if now - last_reload_time < CONFIG_RELOAD_INTERVAL_SECONDS:
        return last_reload_time

    print("\n🔄 Reloader config fra MongoDB...")

    try:
        cfg = cfg_loader.load()
        devices = cfg.get("devices", [])
        em_groups = cfg.get("em_groups", {})

        new_devices = factory.build_device_states(devices, servers)
        new_groups = factory.build_group_states(em_groups, servers)

        # Identify states by stable ids.
        new_device_ids = {("device", st.site) for st in new_devices}
        new_group_ids = {("group", st.group_name) for st in new_groups}

        with states_lock:
            existing_device_ids = {("device", st.site) for w in workers for st in w.device_states}
            existing_group_ids = {("group", st.group_name) for w in workers for st in w.group_states}

            # Remove deleted.
            removed = 0
            for w in workers:
                before = len(w.device_states)
                w.device_states = [st for st in w.device_states if ("device", st.site) in new_device_ids]
                removed += before - len(w.device_states)

                before = len(w.group_states)
                w.group_states = [st for st in w.group_states if ("group", st.group_name) in new_group_ids]
                removed += before - len(w.group_states)

            if removed:
                print(f"🗑️ Fjernet {removed} slettede devices/groups")

            # Add new.
            added = 0
            for st in new_devices:
                if ("device", st.site) not in existing_device_ids:
                    min_worker = min(workers, key=lambda ww: len(ww.device_states) + len(ww.group_states))
                    min_worker.device_states.append(st)
                    added += 1

            for st in new_groups:
                if ("group", st.group_name) not in existing_group_ids:
                    min_worker = min(workers, key=lambda ww: len(ww.device_states) + len(ww.group_states))
                    min_worker.group_states.append(st)
                    added += 1

            if added:
                print(f"✅ Tilføjet {added} nye devices/groups")

            if added or removed:
                total = sum(len(w.device_states) + len(w.group_states) for w in workers)
                print(f"📊 Total states nu: {total}")
            else:
                print("ℹ️ Ingen ændringer i devices/groups")

        return now

    except Exception as e:
        print(f"[ERROR] Fejl ved config reload: {e}")
        return last_reload_time


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def build_reporter(meters_db: Database) -> ErrorReporter:
    """Build a composite reporter: console + (optional) MongoDB events.

    MongoDB persistence uses `service_events` in the Meters database.
    """

    reporters: list[ErrorReporter] = [RateLimitedConsoleReporter()]

    # Enable Mongo event reporting by default. If you prefer console-only, comment this block.
    try:
        reporters.append(ThrottledMongoEventReporter(meters_db["service_events"]))
    except Exception:
        # If Mongo is unavailable, console logging still works.
        pass

    return CompositeReporter(reporters)


def main() -> None:
    """Entry point."""

    print("Starter Modbus Client Manager (refactored)...")

    # Single shared MongoClient for connection pooling
    mongo_client = MongoClient("mongodb://vbserver:27017", serverSelectionTimeoutMS=5000, maxPoolSize=50)

    cfg_loader = MongoConfigLoader(client=mongo_client)
    try:
        cfg = cfg_loader.load()
    except Exception as e:
        print(f"[ERROR] Kan ikke indlæse config: {e}")
        mongo_client.close()
        return

    devices = cfg.get("devices", [])
    em_groups = cfg.get("em_groups", {})
    servers = cfg.get("servers", []) or list(SERVERS)

    print(f"Config: {len(devices)} devices, {len(em_groups)} EM-groups, {len(servers)} servers")

    mongo_db = mongo_client[ENERGY_LOGS_DB]
    meters_db = mongo_client[METERS_DB]

    reporter = build_reporter(meters_db)
    repo = MongoEnergyRepository(mongo_db, reporter=reporter)
    factory = StateFactory(repo)

    validator = EnergyValidator()
    energy_reader = EnergyReader(validator, reporter)

    device_poller = DevicePoller(energy_reader, repo, reporter)
    group_poller = EmGroupPoller(energy_reader, repo, reporter)

    device_states = factory.build_device_states(devices, servers)
    group_states = factory.build_group_states(em_groups, servers)

    total_states = len(device_states) + len(group_states)
    print(f"Total states (devices + EM-groups): {total_states}")

    if total_states == 0:
        print("Ingen states i config – afslutter")
        cfg_loader.close()
        mongo_client.close()
        return

    # Distribute states across workers.
    states_per_worker = 2
    worker_count = max(1, min(200, (total_states + states_per_worker - 1) // states_per_worker))
    print(f"Opretter {worker_count} workers (ca. {states_per_worker} states per)")
    print(f"🔄 Auto-reload aktiveret: Tjekker for nye devices hver {CONFIG_RELOAD_INTERVAL_SECONDS}s (5 min)\n")

    workers: list[Worker] = []
    for i in range(worker_count):
        # Round-robin split, similar to the original.
        dev_chunk = device_states[i::worker_count]
        grp_chunk = group_states[i::worker_count]
        w = Worker(name=f"W{i + 1}", device_states=dev_chunk, group_states=grp_chunk,
                   device_poller=device_poller, group_poller=group_poller)
        w.start()
        workers.append(w)

    last_reload_time = time.time()

    try:
        while any(w.is_alive() for w in workers):
            time.sleep(5)
            last_reload_time = reload_config_if_needed(cfg_loader, repo, factory, workers, last_reload_time, servers)
    except KeyboardInterrupt:
        print("⏹️ Shutdown requested via KeyboardInterrupt")
        shutdown_event.set()
        time.sleep(2)

    cfg_loader.close()
    mongo_client.close()
    print("Program afsluttet.")


if __name__ == "__main__":
    main()
