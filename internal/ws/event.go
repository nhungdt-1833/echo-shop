package ws

import "encoding/json"

type Target string

const (
	TargetUser Target = "user"
	TargetRole Target = "role"
	TargetRoom Target = "room"
)

// Event is a message addressed to "everyone matching Target" across ALL
// server instances. Each instance delivers it to its own local clients.
type Event struct {
	Target Target          `json:"target"`
	UserID uint            `json:"user_id,omitempty"`
	Key    string          `json:"key,omitempty"`     // role or room name
	SkipID string          `json:"skip_id,omitempty"` // client that must not receive it (typing echo)
	Data   json.RawMessage `json:"data"`
}

// Bus spreads events to every instance (Redis Pub/Sub in production).
// Publish must never block: it is called from inside Hub.Run.
// The bus must hand every event back to Hub.Deliver on each instance,
// including the one that published it.
type Bus interface {
	Publish(e Event)
}
