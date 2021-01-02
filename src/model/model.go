package model

// Args defines a type for RPC exchange.
type Args struct {
	ID      string
	Command string
	File    string
}

// Reply defines a type for RPC exchange.
type Reply struct {
	File string
}