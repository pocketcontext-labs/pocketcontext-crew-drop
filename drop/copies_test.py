"""Owner-selected copy on a fenced replacement volume with a synthetic mailbox."""
import http.client
import json
import secrets
import sys
import tempfile
import time
from pathlib import Path
from check import Guest, OWNER, DOMAIN, require


def request(g, method, path, token="", data=None, content_type="application/json", want=200):
    headers = {"Host": DOMAIN, "Authorization": token, "Content-Type": content_type}
    conn = http.client.HTTPConnection("127.0.0.1", g.public, timeout=35)
    body = json.dumps(data).encode() if isinstance(data, dict) else data
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        result = response.read()
        require(response.status == want, f"{method} {path} returned {response.status}, wanted {want}: {result[:250]!r}")
        return result
    finally:
        conn.close()


def login(g, mailbox):
    mailbox.unlink(missing_ok=True)
    sent = json.loads(request(g, "POST", "/api/collections/users/request-otp", data={"email": OWNER}))
    g.until(lambda: mailbox.exists() and len(mailbox.read_bytes()) == 6, "synthetic email code")
    session = json.loads(request(g, "POST", "/api/collections/users/auth-with-otp-code", data={"otpId": sent["otpId"], "code": mailbox.read_text()}))
    return session["token"]


def run(binary, tmp):
    replica, source, target, mailbox = (tmp / name for name in ("replica", "source", "target", "mailbox"))
    env = {"CREW_COPY_TEST_MAILBOX": str(mailbox)}
    endpoint = "/api/workspace/copies"
    copy_id = secrets.token_hex(16)
    body = b"Synthetic protected original\n"
    with Guest(binary, tmp, source, replica, CREW_DROP_BOOTSTRAP="1", **env) as g:
        g.public_ok()
        token = login(g, mailbox)
        boundary = secrets.token_hex(24)
        payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="@jsonPayload"\r\n\r\n'
                   '{"action":"task.create","data":{"title":"Before copy"},"reported_agent":"test"}'
                   f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="original.txt"\r\n'
                   'Content-Type: text/plain\r\n\r\n').encode() + body + f'\r\n--{boundary}--\r\n'.encode()
        task = json.loads(request(g, "POST", "/api/crew/write", token, payload, "multipart/form-data; boundary="+boundary))
        attachment = task["attachments"][0]["id"]
        request(g, "GET", endpoint, want=401)
        g.until(lambda: g.status()["replication_state"] == "ready", "replication ready")
        result=json.loads(request(g,"POST",endpoint,token,{"id":copy_id}))
        require(result["remote"], "funded copy missing remote bytes")
        request(g,"POST",endpoint,token,{"id":copy_id})
        items=json.loads(request(g,"GET",endpoint,token))["copies"]
        require(len(items)==1, "retry created a duplicate")
        request(g,"POST","/api/crew/write",token,{"action":"task.update","id":task["id"],"expected_revision":task["revision"],"data":{"title":"Later edit"},"reported_agent":"test"})
        g.control("/control/sync")
    with Guest(binary,tmp,target,replica,**env) as g:
        g.public_ok()
        token=login(g,mailbox)
        def title():
            return json.loads(request(g,"POST","/api/crew/read",token,{"collection":"tasks","id":task["id"]}))["items"][0]["title"]
        require(title()=="Later edit","fenced replacement did not recover latest records")
        request(g,"POST",endpoint+"/restore",token,{"id":copy_id},want=400)
        require(title()=="Later edit","unconfirmed restore changed records")
        request(g,"POST",endpoint+"/restore",token,{"id":copy_id,"confirm":copy_id},want=202)
        g.until(lambda: not (target/"crew/owner-restore").exists() and g.status()["data_state"]=="ready","selected copy restored")
        token=login(g,mailbox)
        require(title()=="Before copy","selected copy failed to restore earlier record")
        require(request(g,"GET","/api/crew/attachment?id="+attachment,token)==body,"restored attachment differs")
        g.until(lambda: g.status()["replication_state"]=="ready","post-restore replication ready")
        g.control("/control/sync")
    with Guest(binary,tmp,target,None,**env) as g:
        g.public_ok()
        require(request(g,"GET","/api/crew/attachment?id="+attachment,token)==body,"local-only original missing")
        require(not json.loads(request(g,"POST",endpoint,token,{"id":secrets.token_hex(16)}))["remote"],"local-only copy marked remote")


if __name__=="__main__":
    with tempfile.TemporaryDirectory() as d:
        run(Path(sys.argv[1]).resolve(),Path(d))
