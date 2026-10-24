"""In-character messages from the virtual radios, written by the DeepSeek agent.

The agent writes each broadcast in the person's own words from a situation
in messages.txt, fitted to their town and their earlier messages, often
leaving something open for a responder to ask about. If nobody answers an
urgent message, it writes a follow-up.

When a responder (any radio that isn't one of ours) messages a virtual
radio, the agent writes that person's reply: answering questions, giving
location details when asked, staying consistent with what they've already
sent. Pure acknowledgements ("FT-0018 received") get no reply.

The agent's endpoint and access key are read from .env (ENDPOINT, KEY).
The agent doesn't accept system messages, so the role-play instructions go
in the user message.
"""

import re
from pathlib import Path

import requests

ENV_FILE = Path(__file__).parent / ".env"

REQUEST_TIMEOUT_SECONDS = 60
# The model reasons before answering, so leave room for that
MAX_TOKENS = 1500
# A Meshtastic text packet holds about 200 bytes
MAX_REPLY_BYTES = 200

NO_REPLY = "NO_REPLY"

HEADER = """This is a training drill for an emergency communications network. Nothing here is real.

You are role-playing {name}, a civilian in North Georgia during a severe storm. Phones and internet are down, so you are texting over a Meshtastic mesh radio. Emergency responders are on the same network.

Where you are: {place}.

Messages you have sent so far, oldest first:
{sent}
"""

# Rules for every message, whoever it's to
TEXTING_RULES = """- Write like a stressed person texting: short, lowercase is fine, abbreviations and small typos are fine.
- One message, at most 180 characters. No quotes, no explanation, just the text."""

PROMPT = HEADER + """
Your conversation with responders so far, oldest first:
{conversation}

Newest message to you, from responder {sender}:
"{text}"

Write {name}'s reply. Rules:
{answer_rules}
- Stay consistent with what you already sent. Do not invent new emergencies.
- Sometimes ask the responder something back, like a scared person would (how long will it be? should we leave? is anyone else coming?).
""" + TEXTING_RULES + """
- If their message mentions a ticket ID (like FT-0018), start your reply with that ID.
- If their message only acknowledges you and asks nothing, reply with exactly {no_reply}."""

BROADCAST_PROMPT = HEADER + """
The drill script says you report this ({severity}): "{situation}"

Write {name}'s message to everyone on the channel. Rules:
- Use your own words, never the script's wording. Vary your style from your earlier messages: sometimes a fragment, a question, or a run-on.
- Fit it to where you are: swap street names and landmarks for ones in your town, or leave them out.
- If the script is written by a response team or an official, report it as something you saw or heard instead.
- If it fits, connect it to what you already sent (the same flood getting worse, the same neighbor).
- About half the time, leave something open that a responder would want to ask about: a question, a missing detail, or that you're not sure.
{style}
""" + TEXTING_RULES

FOLLOW_UP_PROMPT = HEADER + """
Nobody has answered your message "{text}" for {minutes} minutes.

Write {name}'s follow-up message to everyone on the channel. Rules:
- Something has changed: it got worse, you noticed something new, or you're scared nobody heard you. Pick what fits; don't repeat the earlier message.
{style}
""" + TEXTING_RULES

# How a person with GPS writes broadcasts: the gist, with some details left out
CLEAR_STYLE = """- Say what is happening and roughly where, but about a third of the time leave out one key detail (where exactly, how many, or how bad)."""

# How a person with no GPS writes broadcasts: vague, nothing to locate them by
VAGUE_STYLE = """- Your radio has no GPS. You are panicked and vague: no addresses, street names or numbers. At most a fuzzy landmark ("by the store", "down the hill") or how you feel. Responders should have to ask to find out more."""

# How a person with GPS answers: plainly, with coordinates if asked
CLEAR_RULES = """- Answer what the responder asked. If they ask where you are, give a street, address or landmark consistent with your messages and your location. Give GPS coordinates only if they ask for GPS or coordinates: write {GPS} where they go and they will be filled in for you.
- If they ask for details (how many people, injuries, hazards, what you need), answer plainly. If you honestly would not know, say so."""

# How a person with no GPS answers: vaguely, so responders must ask follow-ups
VAGUE_RULES = """- Your radio has no GPS, so responders cannot see where you are. If asked for GPS or coordinates, say your radio doesn't have that.
- You are panicked and not thinking clearly. Your answers are vague and leave out key details: say things like "near the church", "by the creek", "a few people", "someone's hurt" instead of addresses, numbers or specifics.
- Give one concrete detail only when the responder asks a specific, direct question about it (for example "what street?", "how many people?", "is anyone bleeding?"). A general question like "where are you?" or "what's wrong?" gets another vague answer.
- Never give your full location or situation in one message. Look at the conversation so far and add at most one new specific detail per reply."""


def load_env(path=ENV_FILE):
    """Return KEY=value pairs from .env as a dict (quotes stripped)."""
    text = path.read_text(encoding="utf-8")
    return dict(re.findall(r'^\s*(\w+)\s*=\s*"?([^"\n]*)"?', text, re.M))


class Responder:
    def __init__(self):
        env = load_env()
        self.url = env["ENDPOINT"].rstrip("/") + "/api/v1/chat/completions"
        self.key = env["KEY"]

    def reply(self, virtual, sent, conversation, sender, text):
        """Return the virtual radio's reply to a responder, or None for no reply.

        sent is the list of texts this virtual radio has broadcast.
        conversation is a list of (speaker, text) pairs with responders.
        """
        prompt = PROMPT.format(
            **header_fields(virtual, sent),
            answer_rules=CLEAR_RULES if virtual["gps"] else VAGUE_RULES,
            conversation="\n".join(f"{who}: {line}" for who, line in conversation) or "(none yet)",
            sender=sender,
            text=text,
            no_reply=NO_REPLY,
        )
        answer = self.ask(prompt)
        if not answer or NO_REPLY in answer:
            return None
        # The model gets coordinates wrong, so fill in the real ones
        if virtual["gps"]:
            answer = answer.replace("{GPS}", f"{virtual['lat']:.5f},{virtual['lon']:.5f}")
        return truncate(answer, MAX_REPLY_BYTES)


    def broadcast(self, virtual, sent, situation, severity):
        """Return the person's own wording of a situation from messages.txt, or None."""
        prompt = BROADCAST_PROMPT.format(
            **header_fields(virtual, sent),
            situation=situation,
            severity=severity or "no severity given",
            style=CLEAR_STYLE if virtual["gps"] else VAGUE_STYLE,
        )
        return truncate(self.ask(prompt), MAX_REPLY_BYTES) or None

    def follow_up(self, virtual, sent, text, minutes):
        """Return a follow-up to an urgent message nobody answered, or None."""
        prompt = FOLLOW_UP_PROMPT.format(
            **header_fields(virtual, sent),
            text=text,
            minutes=minutes,
            style=CLEAR_STYLE if virtual["gps"] else VAGUE_STYLE,
        )
        return truncate(self.ask(prompt), MAX_REPLY_BYTES) or None

    def ask(self, prompt):
        """Send the prompt to the agent and return its answer text."""
        response = requests.post(
            self.url,
            headers={"Authorization": f"Bearer {self.key}"},
            json={
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "max_tokens": MAX_TOKENS,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return (response.json()["choices"][0]["message"].get("content") or "").strip().strip('"')


def header_fields(virtual, sent):
    """Return the HEADER fields: who the person is and what they've sent."""
    return {
        "name": virtual["name"],
        "place": virtual["place"],
        "sent": "\n".join(f"- {line}" for line in sent) or "(none)",
    }


def truncate(text, max_bytes):
    """Cut text to at most max_bytes of UTF-8 without splitting a character."""
    return text.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").strip()
