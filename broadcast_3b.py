#!/usr/bin/env python3

from maelstrom import Node, Body, Request

node = Node()
counter = 0
messages = set()
topo = {}


# use some gossip protocol to share the messages
# each node is connected to very other node, easiest is to send it to all the nodes
# for efficiency assume a ring connected and send to only left and right node
# should we guarantee ordering? No: The order of the returned values does not matter.
#   then may be we need a vector clock?
#   does the question ask for it?
#   total order broadcast is the solution
# otherwise just share it with the neightbouring two nodes
# or all neighbours in topology
@node.handler
async def broadcast(req: Request) -> Body:  # it should be named as broadcast (type name of request)
    message = req.body["message"]  # IMP: dict object has no attribute message
    global messages
    messages.add(message)
    # await share_with_neigh(message) # execute in same loop before replying? Yes
    await share_with_all(message)  # execute in same loop before replying? Yes

    return {"type": "broadcast_ok"}


@node.handler
async def read(req: Request) -> Body:
    return {"type": "read_ok", "messages": list(messages)}


@node.handler
async def topology(req: Request) -> Body:
    global topo
    topo = req.body["topology"]
    await node.log("topo=", topo)
    return {"type": "topology_ok"}


async def share_with_all(message):  # This passes test case of 3b
    # Send to all nodes except self
    for neigh in node.node_ids:
        if neigh == node.node_id:
            continue
        await node.log("sending to neigh: ", neigh)
        await node.rpc(neigh, {"type": "gossip3b", "message": message})


async def share_with_neigh(message):
    # This way of sending to neighbours within topo didn't worked
    # check if topo is initialised and then try sending
    if node.node_id in topo:
        for neigh in topo.get(node.node_id):
            await node.log("sending to neigh: ", neigh)
            await node.rpc(neigh, {"type": "gossip3b", "message": message})


@node.handler
async def gossip3b(req: Request) -> Body:
    message = req.body["message"]
    global messages
    await node.log("Updating from gossip: ", message)
    messages.add(message)
    # Do not share with neighbours again here since it might lead to infinite loop
    # Each node takes care of updating its neighbour only when it receives from client
    # Internal gossip from neighbours are not shared with neighbours of current node
    # We can try keeping a message count and do it later and check
    return {"type": "gossip3b_ok"}  # Is this response necessary? It just prints to STDOUT in _send


node.run()

# ./maelstrom/maelstrom test -w broadcast --bin broadcast_3b.py --node-count 5 --time-limit 20 --rate 10

# ./maelstrom/maelstrom serve

# http://localhost:8080/
