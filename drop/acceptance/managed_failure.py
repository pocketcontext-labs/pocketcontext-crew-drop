"""Compare managed Crew and direct Drop sign-in after losing the SMTP DATA reply."""

import re
import time

from smtp_sink import lost_reply_marker


def check_lost_data_reply(mail, *, direct, managed, request, code_for):
    """Endpoints are (port, host, owner) tuples with separate fixture recipients."""
    request_path = "/api/collections/users/request-otp"
    redeem_path = "/api/collections/users/auth-with-otp-code"

    def issue(endpoint):
        port, host, owner = endpoint
        status, body = request(port, host, "POST", request_path, {"email": owner})
        assert status == 200 and set(body) == {"otpId"}, "unexpected OTP response shape"
        assert re.fullmatch(r"[a-z0-9]{15}", body["otpId"]), "unexpected OTP ID format"
        return {"otpId": body["otpId"], "code": code_for(mail, owner)}

    def redeem(endpoint, form):
        port, host, _ = endpoint
        return request(port, host, "POST", redeem_path, form)

    def expect_denied(endpoint, form):
        status, body = redeem(endpoint, form)
        assert status == 400 and set(body) == {"status", "message", "data"}
        # Single- and multi-account Core use different invalid-code messages.
        assert body["status"] == status and body["data"] == {}
        assert isinstance(body["message"], str) and body["message"]

    def submissions():
        return len((mail / "submissions").read_bytes().splitlines())

    endpoints = (direct, managed)
    assert direct[2] != managed[2]
    assert not list(mail.glob("code-*"))
    # Both owners already signed in during this run. Exercise the real minute-long
    # cooldown without restarting a service or changing its rate limits.
    time.sleep(61)
    before = submissions()
    failed = []
    for endpoint in endpoints:
        lost_reply_marker(mail, endpoint[2]).touch(exist_ok=False)
        failed.append(issue(endpoint))

    # Leave both requests alone throughout the normal resend interval. Count MAIL
    # transactions as well as delivered messages to detect even an incomplete retry.
    time.sleep(61)
    assert submissions() == before + 2, "ambiguous submission was retried"
    assert not list(mail.glob("code-*")), "unexpected duplicate delivery"
    assert all(not lost_reply_marker(mail, ep[2]).exists() for ep in endpoints)

    for endpoint, original in zip(endpoints, failed):
        expect_denied(endpoint, original)
        fresh = issue(endpoint)
        assert fresh["otpId"] != original["otpId"], "resend reused the failed request"
        status, auth = redeem(endpoint, fresh)
        assert status == 200 and auth["token"] and auth["record"]["email"] == endpoint[2]
        expect_denied(endpoint, fresh)
    assert submissions() == before + 4, "each explicit request must submit once"
    assert not list(mail.glob("code-*")), "unexpected duplicate delivery"
    print(
        "PASS: direct and managed SMTP lost final reply, failed redemption, "
        "one submission per request and successful one-time resend",
        flush=True,
    )
