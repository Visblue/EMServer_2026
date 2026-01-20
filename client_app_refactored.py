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

SLEEP_SECONDS_RANGE = (2, 3)  # seconds between polls per device (target interval)
DB_SAVE_INTERVAL_SECONDS = 30
CONFIG_RELOAD_INTERVAL_SECONDS = 120  # 2 minutes

MAX_REASONABLE_DIFF_WH = 500_000    # spikes above this are rejected
MAX_REASONABLE_DIFF_WH_EM_GROUP = 1_000_000  # Higher tolerance for EM groups (sum of multiple meters)
MAX_REASONABLE_POWER_W = 1_000_000  # 1 MW
FLOAT_TOLERANCE_WH = 0.01
MIN_TOTAL_VALUE = 0
MAX_TOTAL_RESET_THRESHOLD = 0.5  # meter reset if new < 50% of old

BACKOFF_MIN_SECONDS = 60
BACKOFF_MAX_SECONDS = 360

# Optional per-site debug (to trace specific problematic sites end-to-end).
# Her bruger vi kun server_unit_id for at skelne mellem EM_1 (35) og PV_1 (120) på Bakkegården.
DEBUG_SITES: set[str] = set()
DEBUG_SERVER_UNIT_IDS: set[int] = {35}

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
    - DYNAMIC INTERVAL: Increases interval based on error count to reduce spam:
      - 1-9 errors: 60 seconds
      - 10-49 errors: 5 minutes
      - 50-99 errors: 15 minutes
      - 100-499 errors: 1 hour
      - 500+ errors: 2 hours

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

    def _get_dynamic_interval(self, total_count: int) -> int:
        """Return write interval in seconds based on error count.
        
        Reduces DB writes for persistent/recurring errors.
        Resets after 60 errors to start fresh.
        """
        if total_count < 10:
            return 300         # 5 minutes
        elif total_count < 50:
            return 3600        # 1 hour
        else:
            return 7200        # 2 hours (50-60, then reset)

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        # Normalize the message to group similar errors (e.g., different timestamps)
        normalized_msg = normalize_error_message(event.message)
        key_text = normalized_msg if len(normalized_msg) <= 240 else normalized_msg[:240]
        event_id = f"{event.site}|{event.category}|{key_text}"

        now = time.time()
        st = self._state.setdefault(event_id, {"pending": 0, "last_db": 0.0, "total_count": 0})
        st["pending"] = int(st["pending"]) + 1
        st["total_count"] = int(st["total_count"]) + 1

        # Use dynamic interval based on total error count
        effective_interval = self._get_dynamic_interval(int(st["total_count"]))

        should_write = float(st["last_db"]) == 0.0 or (now - float(st["last_db"]) >= effective_interval)
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
                    "$setOnInsert": {"first_seen": now_dt},
                    "$set": {"last_seen": now_dt, "message": event.message, "severity": event.severity},
                    "$inc": {"count": pending},
                },
                upsert=True,
            )
            st["pending"] = 0
            st["last_db"] = now
            
            # Reset counter after 60 to start fresh cycle
            if int(st["total_count"]) >= 60:
                st["total_count"] = 0
        except Exception as e:
            # Monitoring must never break polling, but log for debugging.
            print(f"[DEBUG] service_events write failed: {e}")


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


import re

# Patterns to normalize in error messages (remove variable parts)
_NORMALIZE_PATTERNS = [
    # Remove "0.000012 seconds" or similar timestamps
    (re.compile(r'\d+\.\d+ seconds'), 'X seconds'),
    # Remove specific byte counts that may vary
    (re.compile(r'read of \d+ bytes'), 'read of N bytes'),
    # Remove IP:port variations in parentheses (keep structure)
    (re.compile(r'ModbusTcpClient\([^)]+\)'), 'ModbusTcpClient(...)'),
]


def normalize_error_message(message: str) -> str:
    """Normalize error message by removing variable parts (timestamps, etc.).
    
    This ensures that errors like:
      "Connection closed 0.000010 seconds into read"
      "Connection closed 0.000012 seconds into read"
    are treated as the same error for counting purposes.
    """
    result = message
    for pattern, replacement in _NORMALIZE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


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


def connect_modbus(ip: str, port: int, timeout_seconds: int = 3) -> Optional[ModbusTcpClient]:
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

        # Detect meter reset or data deletion (new value is much lower)
        if new < last * MAX_TOTAL_RESET_THRESHOLD:
            return True, new, "meter reset eller data nulstillet"

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
            # Info: some devices (fx Bakkegården) har kun active_power-register.
            # I disse tilfælde beregner vi selv total import/export som integreret energi.
            if first_read:
                site = device_cfg.get("site", "unknown")
                try:
                    print(f"[INFO] {site}: ingen total_import/total_export registre – beregner totals ud fra active_power")
                except Exception:
                    pass

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

        # Check if data_collection is explicitly set and not empty
        data_coll = device_cfg.get("data_collection")
        if data_coll and str(data_coll).strip():
            return str(data_coll).strip()
        
        # Fallback: generate from project_nr and name
        project_nr_raw = device_cfg.get("project_nr")
        project = str(project_nr_raw).strip() if project_nr_raw is not None else ""
        name = device_cfg.get("name", "device").strip()
        # Clean name: remove special characters, keep only alphanumeric and underscore
        name_clean = "".join(c if c.isalnum() or c == "_" else "_" for c in name.replace(" ", "_"))
        return f"{project}_{name_clean}" if project else name_clean

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

        site = device_cfg.get("site") or "unknown"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Guard against NaNs.
        if active_power != active_power:
            active_power = 0.0
        if total_import != total_import:
            total_import = last_import
        if total_export != total_export:
            total_export = last_export

        # Ensure totals are not None
        if total_import is None:
            total_import = last_import
        if total_export is None:
            total_export = last_export

        imp_diff = max(0, total_import - last_import) if total_import is not None else 0
        exp_diff = max(0, total_export - last_export) if total_export is not None else 0

        # Ensure all values are not None before saving
        project_nr = device_cfg.get("project_nr") or ""
        device_name = device_cfg.get("name") or ""

        doc = {
            "Site": site,
            "Project_nr": project_nr,
            "Device_name": device_name,
            "Time": now,
            "Active_power": round(active_power, 2),
            "Imported_Wh": imp_diff,
            "Exported_Wh": exp_diff,
            "Total_Imported": total_import,
            "Total_Exported": total_export,
            "Total_Imported_kWh": total_import / 1000 if total_import is not None else 0.0,
            "Total_Exported_kWh": total_export / 1000 if total_export is not None else 0.0,
            "Active_power_valid": not meta.active_power_invalid,
            "Active_power_raw": meta.active_power_raw if meta.active_power_raw is not None else 0.0,
            "Active_power_invalid_reason": meta.active_power_reason or "",
            "Import_valid": not meta.import_invalid,
            "Import_raw": meta.import_raw if meta.import_raw is not None else 0.0,
            "Import_invalid_reason": meta.import_reason or "",
            "Export_valid": not meta.export_invalid,
            "Export_raw": meta.export_raw if meta.export_raw is not None else 0.0,
            "Export_invalid_reason": meta.export_reason or "",
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
    collection_name: str  # Store collection name for reference

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
            # Debug: vis at vi skipper polls for bestemte sites (fx Bakkegården) pga. backoff.
            if state.site in DEBUG_SITES or state.server_unit_id in DEBUG_SERVER_UNIT_IDS:
                try:
                    remaining = max(0.0, state.backoff_until - now)
                    print(f"[DEBUG-BKG] Skipper poll for {state.site} (server_unit_id={state.server_unit_id}) pga. backoff, {remaining:.1f}s tilbage")
                except Exception:
                    pass
            return

        site = state.site
        dev = state.device_cfg
        srv = state.server_cfg

        # Calculate actual interval for display/debugging
        # Use target interval (2-3s) for energy calculations to avoid issues with delays
        raw_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)
        actual_interval = min(raw_interval, 10.0)  # Cap at 10s for display purposes
        target_interval = random.randint(*SLEEP_SECONDS_RANGE)  # Use consistent target for energy calc

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
            if site in DEBUG_SITES or state.server_unit_id in DEBUG_SERVER_UNIT_IDS:
                try:
                    print(f"[DEBUG-BKG] Starter read for {site} (server_unit_id={state.server_unit_id}, interval={actual_interval:.2f}s)")
                except Exception:
                    pass

            ap, total_imp, total_exp, meta = self._energy_reader.read(
                state.device_reader,
                dev,
                state.last_import,
                state.last_export,
                actual_interval,
                first_read=state.first_read,
            )
            state.first_read = False

            if site in DEBUG_SITES or state.server_unit_id in DEBUG_SERVER_UNIT_IDS:
                try:
                    print(
                        f"[DEBUG-BKG] Read OK for {site}: "
                        f"AP={ap:.2f}W, Total_Imp={total_imp:.3f}Wh, Total_Exp={total_exp:.3f}Wh, "
                        f"flags: AP_invalid={meta.active_power_invalid}, "
                        f"Imp_invalid={meta.import_invalid}, Exp_invalid={meta.export_invalid}"
                    )
                except Exception:
                    pass
        except ModbusException as e:
            # Mere detaljeret info om hvor fejlen opstod (hjælper ved fx enkelte sites som Bakkegården).
            dev_ip = dev.get("ip", "unknown")
            dev_port = dev.get("port", "unknown")
            dev_unit = dev.get("unit_id", "unknown")
            msg = f"Modbus fejl på device {site} ({dev_ip}:{dev_port}, unit {dev_unit}): {e}"
            cat, sev = categorize_message(msg)
            self._reporter.record(ErrorEvent(site=site, message=msg, category=cat, severity=sev))
            state.consecutive_errors += 1
            if state.device_reader:
                state.device_reader.close()
            state.device_reader = None
            self._apply_error_backoff(state)
            return
        except Exception as e:
            msg = f"Uventet fejl i read for {site}: {e}"
            cat, sev = categorize_message(msg)
            self._reporter.record(ErrorEvent(site=site, message=msg, category=cat, severity=sev))
            state.consecutive_errors += 1
            self._apply_error_backoff(state)
            return

        # Write values to server.
        try:
            assert state.server_reader is not None
            if site in DEBUG_SITES or state.server_unit_id in DEBUG_SERVER_UNIT_IDS:
                try:
                    print(
                        f"[DEBUG-BKG] Skriver til server for {site} (server_unit_id={state.server_unit_id}): "
                        f"AP={ap:.2f}W, Total_Imp={total_imp:.3f}Wh, Total_Exp={total_exp:.3f}Wh"
                    )
                except Exception:
                    pass

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
        """Print status only when there are invalid readings (errors/warnings)."""

        # Only print when there are actual problems
        if not meta.has_invalid_data:
            return

        # Rate limit console output: max once per 60 seconds per device
        now = time.time()
        if now - state.last_debug_print < 60:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")

        print(
            f"⚠️ {state.site}: "
            f"AP={ap:.1f}W Imp={total_imp:.1f}Wh Exp={total_exp:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={",".join(invalid_flags)})"
        )


class EmGroupPoller:
    """Poller for an EM-group (sum of multiple EM devices -> one server write)."""

    def __init__(self, energy_reader: EnergyReader, repo: MongoEnergyRepository, reporter: ErrorReporter) -> None:
        self._energy_reader = energy_reader
        self._repo = repo
        self._reporter = reporter
        # Create validator for validating summed totals
        self._validator = EnergyValidator()
    
    def _validate_total_with_tolerance(self, new: float, last: float, value_name: str, first_read: bool, max_diff: float) -> tuple[bool, float, str]:
        """Validate total with custom tolerance (for EM groups that sum multiple meters).
        
        Uses higher threshold for EM groups since we're summing values from multiple meters,
        which can result in larger differences.
        """
        if new < MIN_TOTAL_VALUE:
            return False, last, f"{value_name} negativ ({new})"

        if first_read:
            return True, new, "første læsning efter restart"

        if last <= 1.0:
            return True, new, "første læsning"

        # Detect meter reset or data deletion (new value is much lower)
        if new < last * MAX_TOTAL_RESET_THRESHOLD:
            return True, new, "meter reset eller data nulstillet"

        diff = new - last

        if diff < -FLOAT_TOLERANCE_WH:
            return False, last, f"{value_name} faldt ({last} -> {new})"

        # Early phase tolerance.
        if last < 10000 and diff > max_diff:
            extended_limit = max_diff * 10
            if diff > extended_limit:
                return False, last, f"{value_name} spike ({diff} Wh) selv med udvidet grænse"
            return True, new, f"OK (tidlig fase, diff={diff} Wh)"

        if diff > max_diff:
            return False, last, f"{value_name} spike ({diff} Wh)"

        return True, new, "OK"

    def poll_once(self, state: EmGroupState) -> None:
        now = time.time()
        if now < state.backoff_until:
            return

        site = state.site
        srv = state.server_cfg
        # Calculate actual interval for display/debugging
        # Use target interval (2-3s) for energy calculations to avoid issues with delays
        raw_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)
        actual_interval = min(raw_interval, 10.0)  # Cap at 10s for display purposes
        target_interval = random.randint(*SLEEP_SECONDS_RANGE)  # Use consistent target for energy calc

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
                # Use target interval (2-3s) for energy calculation, not actual elapsed time
                target_interval = random.randint(*SLEEP_SECONDS_RANGE)
                ap, imp, exp, meta = self._energy_reader.read(
                    member.reader,
                    em_cfg,
                    0.0,
                    0.0,
                    target_interval,
                    first_read=True,  # Always treat as first read for members - we only validate summed totals
                )
            except ModbusException as e:
                # Mere detaljeret fejltekst for EM-gruppe-medlemmer.
                dev_ip = em_cfg.get("ip", "unknown")
                dev_port = em_cfg.get("port", "unknown")
                dev_unit = em_cfg.get("unit_id", "unknown")
                msg = f"Modbus fejl på EM-device {site}/{em_site} ({dev_ip}:{dev_port}, unit {dev_unit}): {e}"
                cat, sev = categorize_message(msg)
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=msg, category=cat, severity=sev))
                if member.reader:
                    member.reader.close()
                member.reader = None
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue
            except Exception as e:
                msg = f"Uventet fejl i read for EM-device {site}/{em_site}: {e}"
                cat, sev = categorize_message(msg)
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=msg, category=cat, severity=sev))
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue

            member.consecutive_errors = 0

            # Use raw values for summing (not validated values) - validation only happens on the summed total
            # This ensures we sum the actual meter readings, not potentially incorrect validated values
            total_ap += ap
            # Use raw import/export values if available, otherwise use validated values
            imp_value = meta.import_raw if meta.import_raw is not None else imp
            exp_value = meta.export_raw if meta.export_raw is not None else exp
            total_imp_sum += imp_value
            total_exp_sum += exp_value
            success_count += 1

            if meta.active_power_invalid:
                group_meta.active_power_invalid = True
                reasons_ap.append(f"{em_site}: {meta.active_power_reason}")
            group_meta.active_power_raw = float(group_meta.active_power_raw or 0.0) + float(meta.active_power_raw or ap)

            # For import/export, we use raw values for summing and validate at group level
            # Only propagate member-level errors if they're critical (NaN, negative, etc.)
            # Spike/drop validation happens at group level on the summed total
            if meta.import_invalid:
                # Only propagate if it's a critical error (not just a spike/drop)
                if "negativ" in meta.import_reason.lower() or "nan" in meta.import_reason.lower():
                    group_meta.import_invalid = True
                    reasons_imp.append(f"{em_site}: {meta.import_reason}")
            group_meta.import_raw = float(group_meta.import_raw or 0.0) + float(meta.import_raw or imp)

            if meta.export_invalid:
                # Only propagate if it's a critical error (not just a spike/drop)
                if "negativ" in meta.export_reason.lower() or "nan" in meta.export_reason.lower():
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

        # Check if any member has import/export registers configured
        has_import_register = False
        has_export_register = False
        for member in state.members:
            regs = member.cfg.get("registers", {})
            if regs.get("total_import"):
                has_import_register = True
            if regs.get("total_export"):
                has_export_register = True

        # If no import/export registers, calculate from summed active power
        if not has_import_register and not has_export_register:
            # Calculate energy from active power by integrating over time
            effective_interval = max(float(target_interval), 1.0)
            energy_wh = (total_ap * effective_interval) / 3600.0
            
            if total_ap > 0:
                # Positive power = import
                total_imp_sum = state.last_import + energy_wh
                total_exp_sum = state.last_export  # Keep export unchanged
            elif total_ap < 0:
                # Negative power = export
                total_imp_sum = state.last_import  # Keep import unchanged
                total_exp_sum = state.last_export + abs(energy_wh)
            else:
                # Zero power - keep values unchanged
                total_imp_sum = state.last_import
                total_exp_sum = state.last_export
            
            # Update meta to indicate these are calculated values
            group_meta.import_raw = total_imp_sum
            group_meta.export_raw = total_exp_sum
            if not group_meta.import_reason:
                group_meta.import_reason = "Beregnet fra active power"
            if not group_meta.export_reason:
                group_meta.export_reason = "Beregnet fra active power"

        # Validate the summed totals (not individual member values)
        # This ensures we only save valid summed values to the database
        # Track if this is the first successful read for the group
        # Consider it first read if: no previous run, or no historical data, or very long time since last run (>1 hour)
        time_since_last_run = (now - state.last_run_at) if state.last_run_at > 0 else float('inf')
        first_read_for_group = (
            state.run_count == 0 or 
            (state.last_import == 0.0 and state.last_export == 0.0) or
            time_since_last_run > 3600.0  # More than 1 hour since last run
        )
        
        # Validate total_import sum with EM group tolerance (higher threshold since we sum multiple meters)
        if total_imp_sum >= 0:  # Allow 0 values
            # For EM groups, use higher tolerance for spikes
            ok_imp, validated_imp, reason_imp = self._validate_total_with_tolerance(
                total_imp_sum, state.last_import, "Import (sum)", first_read_for_group, MAX_REASONABLE_DIFF_WH_EM_GROUP
            )
            if not ok_imp:
                group_meta.import_invalid = True
                if group_meta.import_reason:
                    group_meta.import_reason += f"; Sum validation failed: {reason_imp}"
                else:
                    group_meta.import_reason = f"Sum validation failed: {reason_imp}"
                cat, sev = categorize_message(f"Import sum invalid: {reason_imp}")
                self._reporter.record(ErrorEvent(site=site, message=f"Import sum invalid: {reason_imp}", category=cat, severity=sev))
                total_imp_sum = state.last_import  # Keep last valid value
            else:
                total_imp_sum = validated_imp
        
        # Validate total_export sum with EM group tolerance
        if total_exp_sum >= 0:  # Allow 0 values
            ok_exp, validated_exp, reason_exp = self._validate_total_with_tolerance(
                total_exp_sum, state.last_export, "Export (sum)", first_read_for_group, MAX_REASONABLE_DIFF_WH_EM_GROUP
            )
            if not ok_exp:
                group_meta.export_invalid = True
                if group_meta.export_reason:
                    group_meta.export_reason += f"; Sum validation failed: {reason_exp}"
                else:
                    group_meta.export_reason = f"Sum validation failed: {reason_exp}"
                cat, sev = categorize_message(f"Export sum invalid: {reason_exp}")
                self._reporter.record(ErrorEvent(site=site, message=f"Export sum invalid: {reason_exp}", category=cat, severity=sev))
                total_exp_sum = state.last_export  # Keep last valid value
            else:
                total_exp_sum = validated_exp

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
            # Ensure project_nr is not None
            project_nr = ""
            if state.members:
                project_nr = state.members[0].cfg.get("project_nr") or ""
            group_device_cfg = {
                "site": state.group_name or "",
                "project_nr": project_nr,
                "name": state.group_name or "",
                "data_collection": state.collection_name,  # Include collection name for reference
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
        """Print status only when there are invalid readings (errors/warnings)."""

        # Only print when there are actual problems
        if not meta.has_invalid_data:
            return

        # Rate limit console output: max once per 60 seconds per group
        now = time.time()
        if now - state.last_debug_print < 60:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")

        print(
            f"⚠️ {state.group_name} (EM-group): "
            f"AP={total_ap:.1f}W Imp={total_imp_sum:.1f}Wh Exp={total_exp_sum:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={",".join(invalid_flags)}, EMs={success_count})"
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

            # Use group_data_collection PRECISELY as specified - do not auto-generate or modify
            # Individual EM devices in a group do NOT need their own data_collection.
            first = group_devices[0]
            # Get group_data_collection - use it PRECISELY as stored in database
            gdc = first.get("group_data_collection")
            if gdc and str(gdc).strip() and str(gdc).strip().lower() != "undefined":
                # Use the EXACT value from database - no modifications
                group_collection_name = str(gdc).strip()
            else:
                # Only auto-generate if completely missing (should not happen if backend validation works)
                # This is a fallback only
                project_nr_raw = first.get("project_nr")
                project_nr = str(project_nr_raw).strip() if project_nr_raw is not None else ""
                device_name = group_name.replace(" ", "_") if group_name else "unknown_group"
                # Clean device name: remove special characters, keep only alphanumeric and underscore
                device_name = "".join(c if c.isalnum() or c == "_" else "_" for c in device_name)
                if project_nr:
                    group_collection_name = f"{project_nr}_{device_name}"
                else:
                    group_collection_name = device_name
                # Log warning if we had to auto-generate (should not happen)
                print(f"⚠️ Warning: Auto-generated collection name for EM group '{group_name}': {group_collection_name}")
            group_device_cfg = {
                "site": group_name,
                "project_nr": first.get("project_nr", ""),
                "name": group_name,
                "data_collection": group_collection_name,
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
                        collection_name=group_collection_name,
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
        
        # Create lookup maps for new states by site/group_name
        new_device_map = {st.site: st for st in new_devices}
        new_group_map = {st.group_name: st for st in new_groups}

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

            # Update existing devices/groups with new config (e.g., IP changes, register changes)
            updated = 0
            for w in workers:
                for i, st in enumerate(w.device_states):
                    if st.site in new_device_map:
                        new_st = new_device_map[st.site]
                        # Check if config has changed (IP, port, registers, etc.)
                        old_cfg = st.device_cfg
                        new_cfg = new_st.device_cfg
                        # Compare key fields that affect polling
                        config_changed = (
                            old_cfg.get("ip") != new_cfg.get("ip") or
                            old_cfg.get("port") != new_cfg.get("port") or
                            old_cfg.get("unit_id") != new_cfg.get("unit_id") or
                            old_cfg.get("reading") != new_cfg.get("reading") or
                            old_cfg.get("table") != new_cfg.get("table")
                        )
                        # Compare registers (simplified - check if active_power address changed)
                        if not config_changed and "registers" in old_cfg and "registers" in new_cfg:
                            old_regs = old_cfg.get("registers", {})
                            new_regs = new_cfg.get("registers", {})
                            if old_regs.get("active_power", {}).get("address") != new_regs.get("active_power", {}).get("address"):
                                config_changed = True
                        
                        if config_changed:
                            # Close old connections to force reconnection with new settings
                            if st.device_reader:
                                st.device_reader.close()
                            if st.server_reader:
                                st.server_reader.close()
                            # Preserve runtime state (last_import, last_export, etc.) from old state
                            new_st.last_import = st.last_import
                            new_st.last_export = st.last_export
                            new_st.last_db_save = st.last_db_save
                            new_st.run_count = st.run_count
                            # Update state with new config
                            w.device_states[i] = new_st
                            updated += 1
                
                for i, st in enumerate(w.group_states):
                    if st.group_name in new_group_map:
                        new_st = new_group_map[st.group_name]
                        # Check if config has changed (members count, IPs, registers, etc.)
                        old_member_cfgs = [m.cfg for m in st.members]
                        new_member_cfgs = [m.cfg for m in new_st.members]
                        
                        # Compare member configs (simplified - check count and key fields)
                        members_changed = len(old_member_cfgs) != len(new_member_cfgs)
                        if not members_changed:
                            for old_m, new_m in zip(old_member_cfgs, new_member_cfgs):
                                if (old_m.get("ip") != new_m.get("ip") or
                                    old_m.get("port") != new_m.get("port") or
                                    old_m.get("unit_id") != new_m.get("unit_id")):
                                    members_changed = True
                                    break
                        
                        if members_changed:
                            # Close old connections
                            if st.server_reader:
                                st.server_reader.close()
                            for m in st.members:
                                if m.reader:
                                    m.reader.close()
                            # Preserve runtime state from old state
                            new_st.last_import = st.last_import
                            new_st.last_export = st.last_export
                            new_st.last_db_save = st.last_db_save
                            new_st.run_count = st.run_count
                            # Update state with new config
                            w.group_states[i] = new_st
                            updated += 1

            if updated:
                print(f"🔄 Opdateret {updated} eksisterende devices/groups med nye indstillinger")

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

            if added or removed or updated:
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
