package dbtesting

import (
	"golang.org/x/crypto/ssh"
	"net"
)

type sshConfig struct {
	network    string
	addr       string
	user       string
	privateKey []byte
}

func (c sshConfig) Dial(addr string) (net.Conn, error) {
	key, err := ssh.ParseRawPrivateKey(c.privateKey)
	if err != nil {
		return nil, err
	}

	s, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return nil, err
	}

	conn, err := ssh.Dial(c.network, c.addr, &ssh.ClientConfig{
		User:            c.user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(s)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		return nil, err
	}

	return conn.Dial("tcp", addr)
}
