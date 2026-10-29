package ws

// Public API, safe from any goroutine. With a bus, notifications reach the
// clients connected to EVERY instance, not only this one.

func (h *Hub) SendToUser(userID uint, m Message) {
	h.publish(UserEvent(userID, m))
}

func (h *Hub) SendToRole(role string, m Message) {
	h.publish(RoleEvent(role, m))
}

func (h *Hub) SendToRoom(room string, m Message) {
	h.publish(Event{Target: TargetRoom, Key: room, Data: encode(m)})
}

// Deliver is called by the bus for every event it receives.
func (h *Hub) Deliver(e Event) {
	h.enqueue(op{kind: opDeliver, event: e})
}

func (h *Hub) publish(e Event) {
	if h.bus != nil {
		h.bus.Publish(e)
		return
	}
	h.Deliver(e)
}

func (h *Hub) sendToClient(c *Client, m Message) {
	h.enqueue(op{kind: opSendClient, client: c, data: encode(m)})
}

func (h *Hub) relay(from *Client, m Message, skipSender bool) {
	h.enqueue(op{kind: opRelay, client: from, key: m.Room, data: encode(m), skipSender: skipSender})
}

func (h *Hub) joinRoom(c *Client, room string)  { h.enqueue(op{kind: opJoin, client: c, key: room}) }
func (h *Hub) leaveRoom(c *Client, room string) { h.enqueue(op{kind: opLeave, client: c, key: room}) }

func (h *Hub) registerClient(c *Client) bool { return send(h, h.register, c) }
func (h *Hub) unregisterClient(c *Client)    { send(h, h.unregister, c) }
func (h *Hub) enqueue(o op)                  { send(h, h.ops, o) }

func send[T any](h *Hub, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-h.done:
		return false
	}
}
