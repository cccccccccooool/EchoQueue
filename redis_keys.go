package echoqueue

import (
	"encoding/base64"
	"fmt"
)

// keyspace is deliberately private. Redis naming is an implementation detail
// of the library and cannot be used as an authorization or identity token.
type keyspace struct {
	prefix string
}

func newKeyspace(namespace string) keyspace {
	return keyspace{prefix: "echoqueue:" + fmt.Sprint(protocolVersion) + ":" + keyPart(namespace)}
}

func (k keyspace) deadline() string {
	return k.prefix + ":deadlines"
}

func (k keyspace) pending(batchID string) string {
	return k.prefix + ":pending:" + keyPart(batchID)
}

func (k keyspace) receipt(batchID string) string {
	return k.prefix + ":receipt:" + keyPart(batchID)
}

func (k keyspace) dead() string {
	return k.prefix + ":dead"
}

func (k keyspace) result(taskName string) string {
	return k.prefix + ":result:" + keyPart(taskName)
}

func (k keyspace) deadFor(taskName string) string {
	return k.prefix + ":dead:" + keyPart(taskName)
}

func keyPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
