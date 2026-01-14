"""Migration script: Add group_data_collection to em_group devices.

This script finds all devices with em_group and adds the group_data_collection
field based on the first device's data_collection in each group.
"""

from pymongo import MongoClient


def migrate_em_groups(dry_run: bool = True) -> None:
    """Migrate em_group devices to use group_data_collection."""
    
    client = MongoClient("mongodb://vbserver:27017", serverSelectionTimeoutMS=5000)
    coll = client["Meters"]["devices"]
    
    # Find all devices with em_group
    em_devices = list(coll.find(
        {"em_group": {"$exists": True, "$ne": None}},
        {"_id": 1, "site": 1, "em_group": 1, "data_collection": 1, "group_data_collection": 1}
    ))
    
    print(f"Fundet {len(em_devices)} EM-group devices")
    
    if not em_devices:
        print("Ingen em_group devices at migrere.")
        client.close()
        return
    
    # Group by em_group name
    groups: dict[str, list[dict]] = {}
    for dev in em_devices:
        group_name = dev["em_group"]
        groups.setdefault(group_name, []).append(dev)
    
    print(f"\nFundet {len(groups)} unikke em_groups:")
    for group_name, members in groups.items():
        print(f"  - {group_name}: {len(members)} devices")
    
    # Migrate each group
    updates_made = 0
    for group_name, members in groups.items():
        # Check if any already has group_data_collection
        existing_gdc = None
        for m in members:
            if m.get("group_data_collection"):
                existing_gdc = m["group_data_collection"]
                break
        
        # Otherwise use first device's data_collection
        if not existing_gdc:
            for m in members:
                if m.get("data_collection"):
                    existing_gdc = m["data_collection"]
                    break
        
        # Fallback to group name
        if not existing_gdc:
            existing_gdc = group_name.replace(" ", "_")
        
        print(f"\n📦 Gruppe: {group_name}")
        print(f"   group_data_collection: {existing_gdc}")
        
        for m in members:
            current_gdc = m.get("group_data_collection")
            if current_gdc == existing_gdc:
                print(f"   ✅ {m['site']}: allerede sat til '{existing_gdc}'")
                continue
            
            if dry_run:
                print(f"   🔄 {m['site']}: ville sætte group_data_collection='{existing_gdc}'")
            else:
                coll.update_one(
                    {"_id": m["_id"]},
                    {"$set": {"group_data_collection": existing_gdc}}
                )
                print(f"   ✅ {m['site']}: opdateret group_data_collection='{existing_gdc}'")
            updates_made += 1
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Total opdateringer: {updates_made}")
    
    if dry_run and updates_made > 0:
        print("\n⚠️  Kør scriptet med dry_run=False for at udføre ændringerne:")
        print("    migrate_em_groups(dry_run=False)")
    
    client.close()


if __name__ == "__main__":
    import sys
    
    dry_run = "--execute" not in sys.argv
    
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - Ingen ændringer vil blive gemt")
        print("Brug --execute for at udføre migreringen")
        print("=" * 60)
    else:
        print("=" * 60)
        print("EXECUTE MODE - Ændringer vil blive gemt i databasen!")
        print("=" * 60)
    
    print()
    migrate_em_groups(dry_run=dry_run)
