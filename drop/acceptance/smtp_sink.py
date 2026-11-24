#!/usr/bin/python3
"""TLS-only, receive-only mailbox for one synthetic test address on a disposable host."""

import base64
import email
import email.policy
import hashlib
import hmac
import os
import re
import socketserver
import ssl
from pathlib import Path

BASE = Path("/run/crew-drop-mail")
OWNER = "owner@crew-drop.invalid"
SENDER = "login@crew-drop.invalid"


class Handler(socketserver.StreamRequestHandler):
    def reply(self, line):
        self.wfile.write(line.encode("ascii") + b"\r\n")

    def handle(self):
        self.connection.settimeout(10)
        self.reply("220 fixture ESMTP")
        authenticated = False
        sender = recipient = False
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
                authenticated = hmac.compare_digest(
                    supplied, b"\x00fixture\x00" + (BASE / "password").read_bytes()
                )
                self.reply("235 authenticated" if authenticated else "535 invalid")
            elif verb == "MAIL" and authenticated:
                sender = arg.lower().split(" ")[0] == "from:<" + SENDER + ">"
                recipient = False
                self.reply("250 sender" if sender else "553 sender denied")
            elif verb == "RCPT" and sender:
                recipient = arg.lower().split(" ")[0] == "to:<" + OWNER + ">"
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
                subject = str(
                    email.message_from_bytes(message, policy=email.policy.default)[
                        "Subject"
                    ]
                )
                code = re.search(r"(?<![0-9])[0-9]{6}(?![0-9])", subject)
                if not code:
                    self.reply("554 synthetic sign-in code missing")
                    return
                received = BASE / (
                    "code-" + hashlib.sha256(os.urandom(32)).hexdigest()[:12]
                )
                received.write_text(code.group() + "\n")
                self.reply("250 captured")
                sender = recipient = False
            elif verb == "QUIT":
                self.reply("221 goodbye")
                return
            elif verb == "RSET":
                sender = recipient = False
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
    with SMTPServer(("0.0.0.0", 2525), Handler) as server:
        server.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server.context.minimum_version = ssl.TLSVersion.TLSv1_2
        server.context.load_cert_chain(BASE / "mail.crt", BASE / "mail.key")
        server.serve_forever()
