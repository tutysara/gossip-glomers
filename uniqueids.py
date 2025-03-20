#!/usr/bin/env python3

from maelstrom import Node, Body, Request

node = Node()
counter = 0

@node.handler
async def generate(req: Request) -> Body: # it should be named as generate (type name of request)
    counter += 1
    new_id = f"{node.node_id}_{counter}"
    return {"type": "generate_ok", "id": new_id}


node.run()

# ./maelstrom/maelstrom test -w unique-ids --bin uniqueids.py --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
