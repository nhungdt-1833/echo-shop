package ws

type clientSet map[*Client]struct{}

func addTo[K comparable](m map[K]clientSet, key K, c *Client) {
	set := m[key]
	if set == nil {
		set = clientSet{}
		m[key] = set
	}
	set[c] = struct{}{}
}

func removeFrom[K comparable](m map[K]clientSet, key K, c *Client) {
	if set := m[key]; set != nil {
		delete(set, c)
		if len(set) == 0 {
			delete(m, key)
		}
	}
}
