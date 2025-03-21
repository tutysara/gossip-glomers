#!/usr/bin/env python3

from maelstrom import Node, Body, Request

node = Node()
counter = 0
messages = []

@node.handler
async def broadcast(req: Request) -> Body: # it should be named as broadcast (type name of request)
    message = req.body["message"] # IMP: dict object has no attribute message
    global messages
    messages.append(message)
    return {"type": "broadcast_ok"}

@node.handler
async def read(req: Request) -> Body:
    global messages
    return {"type": "read_ok", "messages": messages}


@node.handler
async def topology(req: Request) -> Body:
    return {"type": "topology_ok"}


node.run()

# ./maelstrom/maelstrom test -w broadcast --bin broadcast.py --node-count 1 --time-limit 20 --rate 10

# ./maelstrom/maelstrom serve

# http://localhost:8080/
