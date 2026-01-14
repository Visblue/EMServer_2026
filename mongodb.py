"""MongoDB helper script.

This repo historically stored devices inside `customer_meters.config` as a single
document with `config.devices`.

New structure (source of truth)
------------------------------
- Database: `Meters`
- Collection: `devices`
- Each document is a device record, e.g.
  { site, name, ip, port, unit_id, server_unit_id, data_collection, registers, ... }

This script can:
- List devices from `Meters.devices` (default)
- Optionally migrate legacy `customer_meters.config` -> `Meters.devices`
"""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient

MONGO_URI = "mongodb://vbserver:27017/"
METERS_DB = "Meters"
DEVICES_COLL = "devices"

LEGACY_DB = "customer_meters"
LEGACY_COLL = "config"


def list_meters_devices(limit: int = 20) -> None:
    client = MongoClient(MONGO_URI)
    try:
        coll = client[METERS_DB][DEVICES_COLL]
        total = coll.count_documents({})
        print(f"Meters.devices count: {total}")

        for doc in coll.find({}, {"_id": 0}).limit(int(limit)):
            site = doc.get("site")
            name = doc.get("name")
            ip = doc.get("ip")
            port = doc.get("port")
            unit_id = doc.get("unit_id")
            server_unit_id = doc.get("server_unit_id")
            project_nr = doc.get("project_nr")
            data_collection = doc.get("data_collection")
            em_group = doc.get("em_group")
            reading = doc.get("reading")
            registers = doc.get("registers") or {}
            reg_keys = list(registers.keys()) if isinstance(registers, dict) else []

            print(
                f"- {site} / {name} (srv={server_unit_id}, unit={unit_id}, {ip}:{port}, "
                f"project={project_nr}, coll={data_collection}, em_group={em_group}, reading={reading}, regs={reg_keys})"
            )
    finally:
        client.close()


def migrate_legacy_config_to_meters_devices(*, dry_run: bool = True) -> None:
    """Copy devices from legacy `customer_meters.config` into `Meters.devices`.

    This is best-effort and uses a simple upsert key (site + server_unit_id).
    """

    client = MongoClient(MONGO_URI)
    try:
        legacy_doc = client[LEGACY_DB][LEGACY_COLL].find_one({"config": {"$exists": True}}, {"_id": 0})
        if not legacy_doc or "config" not in legacy_doc:
            raise RuntimeError("Legacy config document not found")

        devices: list[dict[str, Any]] = list((legacy_doc.get("config") or {}).get("devices") or [])
        if not devices:
            print("No devices found in legacy config")
            return

        out_coll = client[METERS_DB][DEVICES_COLL]

        created = 0
        updated = 0
        for dev in devices:
            # Keep original dict but avoid carrying Mongo _id across DBs.
            dev = dict(dev)
            dev.pop("_id", None)

            key = {
                "site": dev.get("site"),
                "server_unit_id": dev.get("server_unit_id"),
            }

            if dry_run:
                print(f"[DRY RUN] upsert {key}")
                continue

            res = out_coll.update_one(key, {"$set": dev}, upsert=True)
            if res.upserted_id is not None:
                created += 1
            elif res.modified_count:
                updated += 1

        if dry_run:
            print(f"[DRY RUN] Would upsert {len(devices)} devices")
        else:
            print(f"Migration done. created={created}, updated={updated}")
    finally:
        client.close()


if __name__ == "__main__":
    # Default behavior: list devices from the new database.
    list_meters_devices(limit=25)

    # If you need to migrate legacy data, uncomment:
    # migrate_legacy_config_to_meters_devices(dry_run=True)
    # migrate_legacy_config_to_meters_devices(dry_run=False)
