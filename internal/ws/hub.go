package ws

import "context"

type RoomAuthorizer func(ctx context.Context, userID uint, role, room string) bool

type opKind int

const (
	opSendClient opKind = iota
	opSendUser
	opSendRole
	opSendRoom
	opJoin
	opLeave
)

type op struct {
	kind       opKind
	client     *Client
	userID     uint
	key        string
	data       []byte
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
}

func NewHub(authorize RoomAuthorizer) *Hub {
	return &Hub{
		users:      map[uint]clientSet{},
		roles:      map[string]clientSet{},
		rooms:      map[string]clientSet{},
		register:   make(chan *Client),
		unregister: make(chan *Client),
		ops:        make(chan op, 256),
		done:       make(chan struct{}),
		authorize:  authorize,
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
	case opSendUser:
		h.fanout(h.users[o.userID], o.data, nil)
	case opSendRole:
		h.fanout(h.roles[o.key], o.data, nil)
	case opSendRoom:
		h.sendRoom(o)
	case opJoin:
		h.join(o.client, o.key)
	case opLeave:
		h.leave(o.client, o.key)
	}
}

func (h *Hub) sendRoom(o op) {
	var skip *Client
	if o.client != nil {
		if !h.connected(o.client) {
			return
		}
		if _, ok := o.client.rooms[o.key]; !ok {
			h.trySend(o.client, errorFrame("join the room first"))
			return
		}
		if o.skipSender {
			skip = o.client
		}
	}
	h.fanout(h.rooms[o.key], o.data, skip)
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
	h.fanout(h.rooms[room], encode(Message{Type: TypeJoin, Room: room, UserID: c.userID}), nil)
}

func (h *Hub) leave(c *Client, room string) {
	if _, ok := c.rooms[room]; !ok {
		return
	}
	delete(c.rooms, room)
	removeFrom(h.rooms, room, c)
	h.fanout(h.rooms[room], encode(Message{Type: TypeLeave, Room: room, UserID: c.userID}), nil)
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

func (h *Hub) fanout(set clientSet, data []byte, skip *Client) {
	for c := range set {
		if c != skip {
			h.trySend(c, data)
		}
	}
}

func (h *Hub) trySend(c *Client, data []byte) {
	select {
	case c.send <- data:
	default:
		h.remove(c)
	}
}
