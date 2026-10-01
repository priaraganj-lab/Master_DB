"""Secret scrubbing so credentials never reach logs, audit rows or error tables."""
import re

_URI_CREDS = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^/\s:@]*):(?P<pw>[^@\s/]*)@")
_secrets: set[str] = set()


def register_secrets(*values: str | None) -> None:
    for v in values:
        if v and len(v) >= 4:
            _secrets.add(v)


def scrub(text: object) -> str:
    s = str(text)
    s = _URI_CREDS.sub(lambda m: f"{m['scheme']}{m['user']}:***@", s)
    for secret in _secrets:
        s = s.replace(secret, "***")
    return s
