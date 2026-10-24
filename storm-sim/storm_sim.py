"""Storm simulation: broadcast emergency messages from virtual Meshtastic radios.

Reads messages.txt (one situation per line) and broadcasts one message at a
time on the primary channel, waiting a random 10-60 seconds between
messages. The DeepSeek agent in responder.py writes each message in the
sending person's own words from the situation (the line is sent as it is if
the agent fails). Every radio on the channel receives the text, including
radios not plugged into this laptop.

If nobody messages a person within FOLLOW_UP_SECONDS of a CRITICAL or HIGH
message, they send a follow-up (up to MAX_FOLLOW_UPS), so unanswered calls
for help get louder.

Each real radio has one virtual radio from location.txt (3 virtual radios,
each with its own ID, name and spoofed location): line 1 is Storm A's, line 2
Storm B's, line 3 Storm C's. Each message goes out from a real radio picked
at random, on behalf of that radio's virtual radio. A real radio carries it,
because Meshtastic always stamps the real sender's node ID on a packet. The
virtual radio's details also travel in a hidden packet on the PRIVATE_APP
port, linked to the text with replyId; only this script decodes them.

At startup, each real radio's owner name is changed to its virtual person's
name and announced, so chat apps show that person as the sender. The names
stay the same for the whole run, and the real names (Storm A, ...) are put
back when the script ends.

Each radio is given a fixed position at its virtual radio's location, so it
reports that spot as its own GPS position (the firmware ignores its GPS and
broadcasts the fixed position itself). This uses the set_fixed_position admin
command, which saves without restarting the radio, unlike writing the
position config, which restarted the radios and gave them new node IDs. The
radio's default position is put back when the script ends. The script also
broadcasts a position packet at startup and every POSITION_INTERVAL_SECONDS,
so maps update right away. Maps round positions to the channel's position
precision, and show none if location sharing is off; the script warns about
both.

A virtual radio with no coordinates in location.txt has no GPS: its radio's
fixed position is removed and no position is broadcast, so responders can't
see where that person is. responder.py makes that person's replies vague, so
responders have to ask follow-up questions to locate them.

Real radios are found by name ("Storm A", "Storm B", "Storm C"), so it
doesn't matter which COM port Windows gives them. Missing radios are skipped.
Each radio's default identity (short name, node IDs and fixed position) is
saved in defaults.json whenever it's seen with its real name, so a radio left
with a virtual identity (for example after a crash) is still recognised and
put back. If its node ID changed, it's recognised by the virtual name instead:
the virtual radio on line 1 of location.txt belongs to Storm A, and so on.

When a responder (any radio that isn't ours) messages a virtual radio, the
DeepSeek agent in responder.py writes that person's in-character reply, and
the carrier radio sends it. A message counts as being for a virtual radio if
it's a direct message to its carrier, a reply to one of its messages, or
mentions its first name. After the last message the script keeps answering
responders until Ctrl+C.

Close the browser client, phone app, and other terminals first:
only one program can use a USB port at a time.
"""

import json
import queue
import random
import threading
import time
from pathlib import Path

import meshtastic.serial_interface
import meshtastic.util
from google.protobuf.json_format import ParseDict
from meshtastic.protobuf import config_pb2, mesh_pb2, portnums_pb2
from pubsub import pub

from responder import Responder

RADIO_NAMES = ["Storm A", "Storm B", "Storm C"]

MIN_DELAY_SECONDS = 10
MAX_DELAY_SECONDS = 60
ANNOUNCE_DELAY_SECONDS = 5

MESSAGES_FILE = Path(__file__).parent / "messages.txt"
VIRTUAL_RADIOS_FILE = Path(__file__).parent / "location.txt"
# Real radio name -> {"short_name", "node_ids", "fixed_position"}, so spoofed
# radios can be put back. fixed_position is [lat, lon, alt], or null if none.
DEFAULTS_FILE = Path(__file__).parent / "defaults.json"

# Wait before sending a reply, as if the person is typing
MIN_REPLY_DELAY_SECONDS = 5
MAX_REPLY_DELAY_SECONDS = 15

# Severities whose messages get follow-ups if no responder answers, how long
# to wait, and how many follow-ups at most
FOLLOW_UP_SEVERITIES = {"CRITICAL", "HIGH"}
FOLLOW_UP_SECONDS = 180
MAX_FOLLOW_UPS = 2

# How often each radio re-broadcasts its virtual position, so maps keep it
POSITION_INTERVAL_SECONDS = 600
# Below this channel position precision, maps blur positions by more than
# about 1.5 km (13 is about 2.9 km, 32 is exact)
MIN_POSITION_PRECISION = 14

# Port for the hidden virtual-radio packets. Apps don't display this port.
VIRTUAL_PORT = portnums_pb2.PortNum.PRIVATE_APP

# Node number -> real radio name, filled in after connecting
NODE_NAMES = {}
# Port -> real radio name, so we can say which radio received a message
PORT_NAMES = {}

# Real radio name -> its virtual radio, filled in by main()
IDENTITIES = {}
# Real radio name -> texts its virtual radio has sent, and its conversation
# with responders as (speaker, text, thread) entries. thread is None for the
# public channel, or the responder's node number for a direct message, so
# one responder's DMs aren't shown to the agent when answering anyone else.
SENT = {}
CONVERSATIONS = {}
# Text packet ID -> real radio name, so replies to our messages find their radio
SENT_PACKETS = {}
# Packet IDs already handled (a broadcast reaches every connected radio)
SEEN_PACKETS = set()
# Responder messages waiting for a reply: (carrier name, packet)
REPLY_QUEUE = queue.Queue()
# Only one thread writes to a radio at a time
SEND_LOCK = threading.Lock()
# Real radio name -> when it last broadcast its virtual position
LAST_POSITION = {}
# Real radio name -> its latest urgent message no responder has answered yet:
# {"text", "packet_id", "since", "follow_ups"}
AWAITING = {}


def load_messages(path):
    """Return a list of non-empty lines from the file."""
    with open(path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_virtual_radios(path):
    """Return a list of virtual radios (dicts) from location.txt.

    Each line is: id,name,latitude,longitude,place. Lines starting with #
    are comments. Leave latitude and longitude empty for a radio with no GPS.
    """
    radios = []
    for line in load_messages(path):
        if line.startswith("#"):
            continue
        vid, name, lat, lon, place = line.split(",", 4)
        gps = bool(lat.strip() and lon.strip())
        radios.append({
            "id": vid,
            "name": name,
            "short": short_name_for(name, vid),
            "gps": gps,
            "lat": float(lat) if gps else None,
            "lon": float(lon) if gps else None,
            "place": place,
        })
    return radios


def short_name_for(name, vid):
    """Make a 4-character short name: initials + last 2 digits of the ID.

    "Bubba Snodgrass", "!virt0001" -> "BS01"
    """
    initials = "".join(word[0] for word in name.split())[:2].upper()
    return initials + vid[-2:]


def load_defaults():
    """Return the saved default identities, keyed by real radio name."""
    try:
        return json.loads(DEFAULTS_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}


def save_defaults(defaults):
    DEFAULTS_FILE.write_text(json.dumps(defaults, indent=2), encoding="utf-8")


def default_short_name(name, defaults):
    """Return a real radio's saved short name, or "STA" style for "Storm A"."""
    return defaults.get(name, {}).get("short_name") or "ST" + name[-1]


def rename(interface, long_name, short_name):
    """Change a radio's owner name and announce it to the mesh."""
    interface.localNode.setOwner(long_name, short_name)
    announce(interface, long_name, short_name)


def spoof_position(interface, virtual):
    """Give the radio a fixed position at the virtual radio's location.

    A virtual radio with no GPS gets no position at all.
    """
    if virtual["gps"]:
        interface.localNode.setFixedPosition(virtual["lat"], virtual["lon"], 0)
    else:
        interface.localNode.removeFixedPosition()


def describe_location(virtual):
    """Return "34.52774, -83.97683" or "no GPS" for printing."""
    if virtual.get("lat") is None:
        return "no GPS"
    return f"{virtual['lat']:.5f}, {virtual['lon']:.5f}"


def read_fixed_position(interface):
    """Return the radio's fixed position as [lat, lon, alt], or None if it has none."""
    if not interface.localNode.localConfig.position.fixed_position:
        return None
    position = interface.getMyNodeInfo().get("position", {})
    if position.get("latitude") is None:
        return None
    return [position["latitude"], position["longitude"], position.get("altitude", 0)]


def is_virtual_position(position):
    """True if the position is a virtual radio's location (left by an earlier run)."""
    return position is not None and any(
        abs(position[0] - v["lat"]) < 1e-4 and abs(position[1] - v["lon"]) < 1e-4
        for v in load_virtual_radios(VIRTUAL_RADIOS_FILE) if v["gps"]
    )


def restore_position(name, interface, defaults):
    """Put back the radio's default fixed position, or remove it if it had none.

    Does nothing if the default position was never recorded.
    """
    saved = defaults.get(name, {})
    if "fixed_position" not in saved:
        print(f"    {name}'s default position isn't saved, leaving its position as it is")
    elif saved["fixed_position"]:
        lat, lon, alt = saved["fixed_position"]
        interface.localNode.setFixedPosition(lat, lon, alt)
    else:
        interface.localNode.removeFixedPosition()


def record_defaults(name, interface, defaults):
    """Save the identity of a radio that has its real name.

    Its position is only saved if it isn't a virtual radio's location; if it
    is (an earlier run stopped before restoring it), the default is put back.
    """
    user = interface.getMyNodeInfo()["user"]
    saved = defaults.setdefault(name, {"node_ids": []})
    saved["short_name"] = user.get("shortName")
    if user["id"] not in saved["node_ids"]:
        saved["node_ids"].append(user["id"])
    position = read_fixed_position(interface)
    if is_virtual_position(position):
        print(f"{name} is still at a virtual location, putting its position back")
        restore_position(name, interface, defaults)
    else:
        saved["fixed_position"] = position


def broadcast_position(interface, virtual):
    """Broadcast a position packet at the virtual radio's location."""
    interface.sendPosition(virtual["lat"], virtual["lon"])


def broadcast_positions_if_due(interfaces):
    """Re-broadcast each radio's virtual position every POSITION_INTERVAL_SECONDS."""
    for name, virtual in IDENTITIES.items():
        if not virtual["gps"]:
            continue
        if time.time() - LAST_POSITION.get(name, 0) >= POSITION_INTERVAL_SECONDS:
            with SEND_LOCK:
                broadcast_position(interfaces[name], virtual)
            LAST_POSITION[name] = time.time()


def check_location_sharing(name, interface):
    """Warn if the primary channel hides or blurs positions."""
    precision = interface.localNode.channels[0].settings.module_settings.position_precision
    if precision == 0:
        print(f"Warning: location sharing is off on {name}'s primary channel, so maps won't "
              "show its position. Turn on Position in the app's channel settings.")
    elif precision < MIN_POSITION_PRECISION:
        print(f"Warning: {name}'s primary channel rounds positions (precision {precision}), "
              "so maps may show the virtual radios a few km off or on top of each other. "
              "Set the channel's position to precise in the app for exact locations.")


def check_no_gps(name, interface):
    """Warn if a radio playing a no-GPS person has a GPS that would report its position."""
    if interface.localNode.localConfig.position.gps_mode == config_pb2.Config.PositionConfig.GpsMode.ENABLED:
        print(f"Warning: {name} has its GPS turned on, so it may still broadcast its real "
              "position. Turn GPS off in the app's position settings.")
    print(f"    {name} has no GPS: other radios may still show its last position until it expires")


def real_name_for(user, defaults):
    """Return the real name of a radio left with a virtual name, or None.

    Found by node ID in defaults.json, or if the ID changed, by virtual name:
    the virtual radio on line N of location.txt belongs to RADIO_NAMES[N].
    """
    for real, saved in defaults.items():
        if user["id"] in saved.get("node_ids", []):
            return real
    for real, virtual in zip(RADIO_NAMES, load_virtual_radios(VIRTUAL_RADIOS_FILE)):
        if virtual["name"] == user.get("longName"):
            return real
    return None


def split_severity(message):
    """Split "[CRITICAL] Roof collapsed..." into ("CRITICAL", "Roof collapsed...").

    The tag stays in messages.txt and the terminal, but only the text is sent.
    """
    if message.startswith("[") and "]" in message:
        tag, text = message[1:].split("]", 1)
        return tag, text.strip()
    return "", message


def connect_radios(interfaces):
    """Open every USB radio and add the ones named in RADIO_NAMES to interfaces.

    Fills the dict passed in, so the caller can close any radios that were
    opened even if a later one fails.
    """
    defaults = load_defaults()
    for port in meshtastic.util.findPorts(True):
        interface = meshtastic.serial_interface.SerialInterface(devPath=port)
        user = interface.getMyNodeInfo()["user"]
        name = user.get("longName")

        real = None if name in RADIO_NAMES else real_name_for(user, defaults)
        if real:
            # Left with a virtual identity last time: put the default one back
            print(f"{port} is still named {name!r}, putting back {real}'s default identity")
            interface.localNode.setOwner(real, default_short_name(real, defaults))
            restore_position(real, interface, defaults)
            saved = defaults.setdefault(real, {"node_ids": []})
            if user["id"] not in saved["node_ids"]:
                saved["node_ids"].append(user["id"])
            name = real
        elif name in RADIO_NAMES:
            record_defaults(name, interface, defaults)

        if name in RADIO_NAMES:
            print(f"Found {name} on {port}")
            interfaces[name] = interface
            NODE_NAMES[interface.myInfo.my_node_num] = name
            PORT_NAMES[port] = name
        else:
            print(f"Skipping {port}: radio is named {name!r}")
            interface.close()
    save_defaults(defaults)

    for name in RADIO_NAMES:
        if name not in interfaces:
            print(f"Warning: {name} is not plugged in, continuing without it")


def sender_name(packet):
    # "from" (the node number) is always there, unlike "fromId"
    return NODE_NAMES.get(packet["from"], packet.get("fromId") or packet["from"])


def on_receive(packet, interface):
    """Called by meshtastic whenever a radio receives a text message."""
    text = packet["decoded"]["text"]
    receiver = PORT_NAMES.get(interface.devPath, interface.devPath)
    print(f"    {receiver} received from {sender_name(packet)}: {text}")

    if packet["from"] in NODE_NAMES or packet["id"] in SEEN_PACKETS:
        return  # one of our own radios, or already handled
    carrier = addressed_carrier(packet, receiver)
    if carrier:
        SEEN_PACKETS.add(packet["id"])
        AWAITING.pop(carrier, None)  # a responder answered, no follow-up needed
        REPLY_QUEUE.put((carrier, packet))


def node_num(name):
    """Return a connected real radio's node number."""
    return next(num for num, real in NODE_NAMES.items() if real == name)


def addressed_carrier(packet, receiver):
    """Return the real radio whose virtual radio a responder's message is for, or None."""
    if receiver in IDENTITIES and packet["to"] == node_num(receiver):
        return receiver  # direct message to this radio
    reply_to = packet["decoded"].get("replyId")
    if reply_to in SENT_PACKETS:
        return SENT_PACKETS[reply_to]
    text = packet["decoded"]["text"].lower()
    for carrier, virtual in IDENTITIES.items():
        if virtual["name"].split()[0].lower() in text:
            return carrier
    return None


def answer_responders(interfaces, responder):
    """Worker thread: write and send each virtual radio's reply, one at a time."""
    while True:
        carrier, packet = REPLY_QUEUE.get()
        virtual = IDENTITIES[carrier]
        text = packet["decoded"]["text"]
        sender = sender_name(packet)
        # Reply the way they wrote: privately to a direct message, else on the channel
        direct = packet["to"] == node_num(carrier)
        thread = packet["from"] if direct else None
        # The agent sees the public channel plus this DM only, never other DMs
        visible = [(who, line) for who, line, t in CONVERSATIONS[carrier] if t in (None, thread)]
        started = time.time()
        try:
            reply = responder.reply(virtual, SENT[carrier], visible, sender, text)
        except Exception as error:
            print(f"    could not write {virtual['name']}'s reply: {error}")
            continue
        CONVERSATIONS[carrier].append((sender, text, thread))
        if reply is None:
            print(f"    {virtual['name']} doesn't need to answer {sender}")
            continue

        typing = random.uniform(MIN_REPLY_DELAY_SECONDS, MAX_REPLY_DELAY_SECONDS)
        time.sleep(max(0, typing - (time.time() - started)))
        print(f"    {virtual['name']} (via {carrier}) answers {sender}{' privately' if direct else ''}: {reply}")
        send_as_virtual(
            interfaces[carrier], reply, virtual, carrier,
            destination=packet["from"] if direct else "^all",
            reply_id=packet["id"],
        )
        CONVERSATIONS[carrier].append(("you", reply, thread))


def write_message(responder, carrier, severity, situation):
    """Return the person's own wording of a situation, or the situation as it is if that fails."""
    try:
        return responder.broadcast(IDENTITIES[carrier], SENT[carrier], situation, severity) or situation
    except Exception as error:
        print(f"    could not write the message, sending the script line: {error}")
        return situation


def send_follow_ups_if_due(interfaces, responder):
    """Send a follow-up for each urgent message nobody has answered in FOLLOW_UP_SECONDS."""
    for carrier, waiting in list(AWAITING.items()):
        if time.time() - waiting["since"] < FOLLOW_UP_SECONDS:
            continue
        virtual = IDENTITIES[carrier]
        minutes = round((time.time() - waiting["since"]) / 60)
        try:
            text = responder.follow_up(virtual, SENT[carrier], waiting["text"], minutes)
        except Exception as error:
            print(f"    could not write {virtual['name']}'s follow-up: {error}")
            text = None
        if AWAITING.get(carrier) is not waiting:
            continue  # answered (or replaced) while the follow-up was being written
        if text:
            print(f"    {virtual['name']} (via {carrier}) follows up, nobody answered: {text}")
            send_as_virtual(interfaces[carrier], text, virtual, carrier, reply_id=waiting["packet_id"])
            SENT[carrier].append(text)
        waiting["follow_ups"] += 1
        waiting["since"] = time.time()
        if waiting["follow_ups"] >= MAX_FOLLOW_UPS:
            AWAITING.pop(carrier, None)


def on_data(packet, interface):
    """Called for packets on unknown ports. Decode our hidden virtual-radio info."""
    decoded = packet["decoded"]
    if decoded.get("portnum") not in ("PRIVATE_APP", VIRTUAL_PORT):
        return
    try:
        info = json.loads(decoded["payload"])
    except ValueError:
        return  # someone else's PRIVATE_APP data, not ours
    receiver = PORT_NAMES.get(interface.devPath, interface.devPath)
    print(
        f"    {receiver}: that message came from virtual radio {info['name']!r} ({info['id']}) "
        f"at {describe_location(info)}, relayed by {sender_name(packet)}"
    )


def on_routing(packet, interface):
    """Called when a delivery report comes back. Print only failures."""
    error = packet["decoded"]["routing"].get("errorReason", "NONE")
    if error != "NONE":
        sender = PORT_NAMES.get(interface.devPath, interface.devPath)
        print(f"    a message sent by {sender} was not delivered: {error}")


def announce(interface, long_name=None, short_name=None):
    """Broadcast this radio's node info (including its public key).

    This lets every radio on the mesh (including a friend's) show the radio's
    current name instead of its ID, and share public keys in case we send
    direct messages later. Pass long_name/short_name right after a rename,
    because the library's copy of the name only updates later.
    """
    user = dict(interface.getMyNodeInfo()["user"])
    user.pop("macaddr", None)  # not in a format ParseDict accepts
    if long_name:
        user["longName"] = long_name
        user["shortName"] = short_name
    interface.sendData(
        ParseDict(user, mesh_pb2.User(), ignore_unknown_fields=True),
        portNum=portnums_pb2.PortNum.NODEINFO_APP,
    )


def send_as_virtual(interface, text, virtual, carrier, destination="^all", reply_id=None):
    """Send the text, then the hidden virtual-radio info linked by replyId.

    "^all" broadcasts to everyone on the channel. reply_id marks the text as
    a reply to that packet. Returns the text packet's ID.
    """
    with SEND_LOCK:
        text_packet = interface.sendText(text, destinationId=destination, replyId=reply_id)
        SENT_PACKETS[text_packet.id] = carrier
        send_virtual_info(interface, virtual, destination, text_packet.id)
    return text_packet.id


def send_virtual_info(interface, virtual, destination, text_id):
    """Send the hidden virtual-radio info, linked to the text packet text_id."""
    info = {
        "id": virtual["id"],
        "name": virtual["name"],
        "lat": virtual["lat"],
        "lon": virtual["lon"],
        "time": int(time.time()),
    }
    # Compact JSON keeps the packet small (about 100 bytes)
    payload = json.dumps(info, separators=(",", ":")).encode("utf-8")
    interface.sendData(payload, destinationId=destination, portNum=VIRTUAL_PORT, replyId=text_id)


def main():
    messages = load_messages(MESSAGES_FILE)
    virtual_radios = load_virtual_radios(VIRTUAL_RADIOS_FILE)
    print(f"Loaded {len(messages)} messages and {len(virtual_radios)} virtual radios")
    if len(virtual_radios) < len(RADIO_NAMES):
        print(f"{VIRTUAL_RADIOS_FILE.name} needs one virtual radio per real radio ({len(RADIO_NAMES)})")
        return
    responder = Responder()

    # Print every text message and hidden virtual-radio packet any connected radio receives
    pub.subscribe(on_receive, "meshtastic.receive.text")
    pub.subscribe(on_data, "meshtastic.receive.data")
    pub.subscribe(on_routing, "meshtastic.receive.routing")

    interfaces = {}
    try:
        connect_radios(interfaces)
        if not interfaces:
            print("No Storm radios found")
            return

        # Rename (and announce) each radio as its virtual radio, once for the whole run.
        # Virtual radios go to real radios by line order in location.txt.
        for name, virtual in zip(RADIO_NAMES, virtual_radios):
            if name not in interfaces:
                continue
            IDENTITIES[name] = virtual
            SENT[name] = []
            CONVERSATIONS[name] = []
            print(f"{name} is virtual radio {virtual['name']} ({virtual['id']})")
            check_location_sharing(name, interfaces[name])
            if not virtual["gps"]:
                check_no_gps(name, interfaces[name])
            with SEND_LOCK:
                rename(interfaces[name], virtual["name"], virtual["short"])
                spoof_position(interfaces[name], virtual)
            time.sleep(ANNOUNCE_DELAY_SECONDS)
        broadcast_positions_if_due(interfaces)

        threading.Thread(target=answer_responders, args=(interfaces, responder), daemon=True).start()

        for number, message in enumerate(messages, start=1):
            broadcast_positions_if_due(interfaces)
            send_follow_ups_if_due(interfaces, responder)
            carrier = random.choice(list(interfaces))
            virtual = IDENTITIES[carrier]
            severity, situation = split_severity(message)
            text = write_message(responder, carrier, severity, situation)
            print(f"[{number}/{len(messages)}] {severity:<8}  {virtual['name']} (via {carrier}): {text}")
            print(f"    script: {situation}")
            print(f"    virtual radio {virtual['id']} at {describe_location(virtual)} ({virtual['place']})")
            packet_id = send_as_virtual(interfaces[carrier], text, virtual, carrier)
            SENT[carrier].append(text)
            if severity in FOLLOW_UP_SEVERITIES:
                AWAITING[carrier] = {"text": text, "packet_id": packet_id, "since": time.time(), "follow_ups": 0}

            if number < len(messages):
                delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                print(f"    next message in {delay:.0f} s")
                time.sleep(delay)

        print("Done sending all messages. Still answering responders, press Ctrl+C to stop")
        while True:
            broadcast_positions_if_due(interfaces)
            send_follow_ups_if_due(interfaces, responder)
            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopped by user")

    finally:
        defaults = load_defaults()
        for name, interface in interfaces.items():
            print(f"Putting back {name}'s default identity and closing")
            try:
                with SEND_LOCK:
                    rename(interface, name, default_short_name(name, defaults))
            except Exception as error:
                print(f"    could not rename {name} back: {error}")
            if name in IDENTITIES:
                try:
                    with SEND_LOCK:
                        restore_position(name, interface, defaults)
                except Exception as error:
                    print(f"    could not restore {name}'s position: {error}")
            interface.close()


if __name__ == "__main__":
    main()
