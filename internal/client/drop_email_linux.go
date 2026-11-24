//go:build linux && crew_drop

package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/mdlayher/socket"
	"golang.org/x/sys/unix"
)

// Port allocated by NibRunner's guest-contract crate.
const dropSignInPort = 51006

var errDropMail = errors.New("managed sign-in email submission did not complete")

func sendDropSignInCode(ctx context.Context, code string) error {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	conn, err := socket.Socket(unix.AF_VSOCK, unix.SOCK_STREAM, 0, "sign-in-email", nil)
	if err != nil {
		return errDropMail
	}
	defer conn.Close()
	if _, err := conn.Connect(ctx, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: dropSignInPort}); err != nil {
		return errDropMail
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return errDropMail
	}
	return submitDropCode(conn, code)
}

func submitDropCode(conn io.ReadWriter, code string) error {
	if len(code) != 6 {
		return errDropMail
	}
	for _, ch := range code {
		if ch < '0' || ch > '9' {
			return errDropMail
		}
	}
	if err := json.NewEncoder(conn).Encode(struct {
		Code string `json:"code"`
	}{code}); err != nil {
		return errDropMail
	}
	// Read exactly one bounded acknowledgement; never resend an uncertain submission.
	line, err := bufio.NewReader(io.LimitReader(conn, 128)).ReadBytes('\n')
	var reply struct {
		Status string `json:"status"`
	}
	if err != nil || json.Unmarshal(line, &reply) != nil || reply.Status != "accepted" {
		return errDropMail
	}
	return nil
}
