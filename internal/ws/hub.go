package ws

import "context"

type RoomAuthorizer func(ctx context.Context, userID uint, role, room string) bool

type opKind int

const (
	opSendClient opKind = iota // local error frame to one connection
	opDeliver                  // event to deliver to local clients
	opRelay                    // chat/typing sent by a local client
	opJoin
	opLeave
)

type op struct {
	kind       opKind
	client     *Client
	key        string
	data       []byte
	event      Event
	skipSender bool
}

type Hub struct {
	users map[uint]clientSet
	roles map[string]clientSet
	rooms map[string]clientSet

	register   chan *Client
	unregister chan *Client
	ops        chan op
	done       chan struct{}

	authorize RoomAuthorizer
	bus       Bus // nil = single instance, deliver locally
}

func NewHub(authorize RoomAuthorizer, bus Bus) *Hub {
	return &Hub{
		users:      map[uint]clientSet{},
		roles:      map[string]clientSet{},
		rooms:      map[string]clientSet{},
		register:   make(chan *Client),
		unregister: make(chan *Client),
		ops:        make(chan op, 256),
		done:       make(chan struct{}),
		authorize:  authorize,
		bus:        bus,
	}
}

func (h *Hub) Run(ctx context.Context) {
	defer close(h.done)
	for {
		select {
		case <-ctx.Done():
			for _, set := range h.users {
				for c := range set {
					h.remove(c)
				}
			}
			return
		case c := <-h.register:
			addTo(h.users, c.userID, c)
			addTo(h.roles, c.role, c)
		case c := <-h.unregister:
			h.remove(c)
		case o := <-h.ops:
			h.apply(o)
		}
	}
}

func (h *Hub) Done() <-chan struct{} { return h.done }

func (h *Hub) apply(o op) {
	switch o.kind {
	case opSendClient:
		if h.connected(o.client) {
			h.trySend(o.client, o.data)
		}
	case opDeliver:
		h.deliverLocal(o.event)
	case opRelay:
		h.relayFrom(o)
	case opJoin:
		h.join(o.client, o.key)
	case opLeave:
		h.leave(o.client, o.key)
	}
}

// emit is the in-Run version of publishing: through the bus when there is
// one (other instances must see it too), otherwise straight to local clients.
func (h *Hub) emit(e Event) {
	if h.bus != nil {
		h.bus.Publish(e)
		return
	}
	h.deliverLocal(e)
}

func (h *Hub) deliverLocal(e Event) {
	var set clientSet
	switch e.Target {
	case TargetUser:
		set = h.users[e.UserID]
	case TargetRole:
		set = h.roles[e.Key]
	case TargetRoom:
		set = h.rooms[e.Key]
	}
	for c := range set {
		if c.id != e.SkipID {
			h.trySend(c, e.Data)
		}
	}
}

// relayFrom checks membership HERE, on the sender's instance, because only
// this instance knows which rooms its own clients joined.
func (h *Hub) relayFrom(o op) {
	c := o.client
	if !h.connected(c) {
		return
	}
	if _, ok := c.rooms[o.key]; !ok {
		h.trySend(c, errorFrame("join the room first"))
		return
	}
	e := Event{Target: TargetRoom, Key: o.key, Data: o.data}
	if o.skipSender {
		e.SkipID = c.id
	}
	h.emit(e)
}

func (h *Hub) join(c *Client, room string) {
	if !h.connected(c) {
		return
	}
	if _, ok := c.rooms[room]; ok {
		return
	}
	c.rooms[room] = struct{}{}
	addTo(h.rooms, room, c)
	h.emit(Event{Target: TargetRoom, Key: room, Data: encode(Message{Type: TypeJoin, Room: room, UserID: c.userID})})
}

func (h *Hub) leave(c *Client, room string) {
	if _, ok := c.rooms[room]; !ok {
		return
	}
	delete(c.rooms, room)
	removeFrom(h.rooms, room, c)
	h.emit(Event{Target: TargetRoom, Key: room, Data: encode(Message{Type: TypeLeave, Room: room, UserID: c.userID})})
}

func (h *Hub) remove(c *Client) {
	if !h.connected(c) {
		return
	}
	removeFrom(h.users, c.userID, c)
	removeFrom(h.roles, c.role, c)
	close(c.send)
	for room := range c.rooms {
		h.leave(c, room)
	}
}

func (h *Hub) connected(c *Client) bool {
	_, ok := h.users[c.userID][c]
	return ok
}

func (h *Hub) trySend(c *Client, data []byte) {
	select {
	case c.send <- data:
	default:
		h.remove(c)
	}
}
