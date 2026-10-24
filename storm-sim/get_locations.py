"""Read the location of each Meshtastic radio and save it to radio_locations.csv.

Each radio reports its own position over USB: from GPS if it has one,
otherwise the fixed position set with --setlat / --setlon.

Radios are found by name, the same way storm_sim.py finds them.

Close storm_sim.py, the browser client and the phone app first:
only one program can use a USB port at a time.
"""

import csv
from datetime import datetime
from pathlib import Path

from storm_sim import connect_radios

OUTPUT_FILE = Path(__file__).parent / "radio_locations.csv"
COLUMNS = ["radio", "port", "node_id", "latitude", "longitude", "altitude_m", "source", "read_at"]


def read_location(name, interface):
    """Return one connected radio's location as a dict."""
    info = interface.getMyNodeInfo()
    position = info.get("position", {})
    fixed = interface.localNode.localConfig.position.fixed_position
    return {
        "radio": name,
        "port": interface.devPath,
        "node_id": info["user"]["id"],
        "latitude": position.get("latitude"),
        "longitude": position.get("longitude"),
        "altitude_m": position.get("altitude"),
        "source": "fixed position" if fixed else "GPS",
        "read_at": datetime.now().isoformat(timespec="seconds"),
    }


def main():
    interfaces = {}
    rows = []
    try:
        connect_radios(interfaces)
        for name in sorted(interfaces):
            row = read_location(name, interfaces[name])
            if row["latitude"] is None:
                print(f"    {name} has no position yet")
            else:
                print(f"    {name}: {row['latitude']}, {row['longitude']} ({row['source']})")
            rows.append(row)
    finally:
        for interface in interfaces.values():
            interface.close()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} locations to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
