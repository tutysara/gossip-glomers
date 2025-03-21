#!/usr/bin/env python3

from maelstrom import Node, Body, Request
import time

node = Node()
counter = 0

# we have to generate a unique id on each node
# node_id is unique among nodes
# so, we can generate a monotonically increasing number in the current node
# each node is single threaded so a global counter works without synchronization


def generate_id_using_global_counter(): # 11213
    global counter
    counter += 1
    id = f"{node.node_id}_{counter}"
    return id

def generate_id_using_monotonic_clock(): # 11725
    cur_mono_val = time.monotonic()
    return f"{node.node_id}_{cur_mono_val}"

@node.handler
async def generate(req: Request) -> Body: # it should be named as generate (type name of request)
    unique_id = generate_id_using_global_counter() 
    return {"type": "generate_ok", "id": unique_id}

node.run()

# ./maelstrom/maelstrom test -w unique-ids --bin uniqueids.py --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
# ./maelstrom/maelstrom serve

# http://localhost:8080/

# issue with counter not initialised https://stackoverflow.com/questions/74412503/cannot-access-local-variable-a-where-it-is-not-associated-with-a-value-but