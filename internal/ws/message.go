package ws

import "encoding/json"

const (
	TypeOrderStatus = "order_status"
	TypeNewOrder    = "new_order"
	TypeJoin        = "join"
	TypeLeave       = "leave"
	TypeTyping      = "typing"
	TypeChat        = "chat"
	TypeError       = "error"
)

type Message struct {
	Type    string `json:"type"`
	Room    string `json:"room,omitempty"`
	UserID  uint   `json:"user_id,omitempty"`
	OrderID uint   `json:"order_id,omitempty"`
	Status  string `json:"status,omitempty"`
	Total   int64  `json:"total,omitempty"`
	Text    string `json:"text,omitempty"`
	Error   string `json:"error,omitempty"`
}

func encode(m Message) []byte {
	data, _ := json.Marshal(m)
	return data
}

func errorFrame(text string) []byte {
	return encode(Message{Type: TypeError, Error: text})
}
