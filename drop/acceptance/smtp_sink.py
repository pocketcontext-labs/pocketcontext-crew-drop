#!/usr/bin/python3
"""TLS-only, receive-only mailbox for synthetic test addresses on a disposable host."""

import argparse
import base64
import email
import email.policy
import hmac
import os
import re
import secrets
import socketserver
import ssl
from pathlib import Path

BASE = Path("/run/crew-drop-mail")
OWNER = "owner@crew-drop.invalid"
SENDER = "login@crew-drop.invalid"


def lost_reply_marker(base, recipient):
    return base / f"lose-next-data-reply-{recipient}"


class Handler(socketserver.StreamRequestHandler):
    def reply(self, line):
        self.wfile.write(line.encode("ascii") + b"\r\n")

    def handle(self):
        self.connection.settimeout(10)
        self.reply("220 fixture ESMTP")
        authenticated = None
        sender = recipient = ""
        for _ in range(24):
            line = self.rfile.readline(2048).decode("ascii", "replace").strip()
            if not line:
                return
            verb, _, arg = line.partition(" ")
            if verb in ("EHLO", "HELO"):
                self.reply("250-fixture")
                self.reply("250 AUTH PLAIN")
            elif verb == "AUTH":
                scheme, _, payload = arg.partition(" ")
                if scheme.upper() != "PLAIN":
                    self.reply("504 PLAIN required")
                    continue
                if not payload:
                    self.reply("334")
                    payload = self.rfile.readline(512).strip()
                try:
                    supplied = base64.b64decode(payload, validate=True)
                except ValueError:
                    supplied = b""
                authenticated = next(
                    (
                        sender
                        for sender, expected in self.server.credentials.items()
                        if hmac.compare_digest(supplied, expected)
                    ),
                    None,
                )
                self.reply("235 authenticated" if authenticated else "535 invalid")
            elif verb == "MAIL" and authenticated:
                with (self.server.base / "submissions").open("ab") as counter:
                    counter.write(b"1\n")
                sender = (
                    authenticated
                    if arg.lower().split(" ")[0] == f"from:<{authenticated}>"
                    else ""
                )
                recipient = ""
                self.reply("250 sender" if sender else "553 sender denied")
            elif verb == "RCPT" and sender:
                recipient = next(
                    (
                        address
                        for address in self.server.owners
                        if arg.lower().split(" ")[0] == f"to:<{address}>"
                    ),
                    "",
                )
                self.reply("250 recipient" if recipient else "550 recipient denied")
            elif verb == "DATA" and recipient:
                self.reply("354 end with dot")
                message = bytearray()
                while len(message) <= 65536:
                    fragment = self.rfile.readline(8192)
                    if fragment == b".\r\n":
                        break
                    if not fragment:
                        return
                    message.extend(fragment)
                else:
                    self.reply("552 message too large")
                    return
                msg = email.message_from_bytes(message, policy=email.policy.default)
                if sender == SENDER:
                    plain = msg.get_body(preferencelist=("plain",))
                    body = (
                        plain.get_content().replace("\r\n", "\n").rstrip("\n")
                        if plain
                        else ""
                    )
                    code = re.fullmatch(
                        r"Your sign-in code is ([0-9]{6})\.\n\nEnter this code in the app to sign in\.",
                        body,
                    )
                    if (
                        not code
                        or msg.get_content_type() != "text/plain"
                        or len(msg.get_all("From", [])) != 1
                        or len(msg.get_all("To", [])) != 1
                        or str(msg["Subject"]) != "Your sign-in code"
                        or [a.addr_spec for a in msg["From"].addresses] != [sender]
                        or [a.addr_spec for a in msg["To"].addresses] != [recipient]
                    ):
                        self.reply("554 unexpected managed sign-in message")
                        continue
                else:
                    code = re.fullmatch(
                        r"([0-9]{6}) is your Drop sign-in code", str(msg["Subject"])
                    )
                    if not code:
                        self.reply("554 unexpected control plane message")
                        continue
                # Publish the private test mailbox atomically before acknowledging SMTP DATA.
                nonce = secrets.token_hex(8)
                pending = self.server.base / ("pending-" + nonce)
                pending.write_text(code[1] + "\n")
                pending.replace(self.server.base / f"code-{recipient}-{nonce}")
                # Consume a single fault even if another SMTP session is in flight.
                try:
                    lost_reply_marker(self.server.base, recipient).unlink()
                except FileNotFoundError:
                    pass
                else:
                    return
                self.reply("250 captured")
                sender = recipient = ""
            elif verb == "QUIT":
                self.reply("221 goodbye")
                return
            elif verb == "RSET":
                sender = recipient = ""
                self.reply("250 reset")
            elif verb == "NOOP":
                self.reply("250 ok")
            else:
                self.reply("503 command not available")


class SMTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def get_request(self):
        sock, addr = super().get_request()
        sock.settimeout(10)
        try:
            return self.context.wrap_socket(sock, server_side=True), addr
        except Exception:
            sock.close()
            raise


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, default=BASE)
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=2525)
    parser.add_argument("--owner", action="append", default=[])
    parser.add_argument("--drop-sender")
    args = parser.parse_args()
    with SMTPServer((args.listen, args.port), Handler) as server:
        server.base = args.base
        server.credentials = {
            SENDER: b"\x00fixture\x00" + (args.base / "password").read_bytes().strip()
        }
        server.owners = set(args.owner or [OWNER])
        if args.drop_sender:
            server.credentials[args.drop_sender] = (
                b"\x00fixture-drop\x00"
                + (args.base / "drop-password").read_bytes().strip()
            )
        server.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server.context.minimum_version = ssl.TLSVersion.TLSv1_2
        server.context.load_cert_chain(args.base / "mail.crt", args.base / "mail.key")
        server.serve_forever()
