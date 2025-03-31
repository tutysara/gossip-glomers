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
    await share_with_neigh(message) # execute in same loop before replying? Yes

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


async def share_with_neigh(message):
    # This way of sending to neighbours within topo didn't worked
    # check if topo is initialised and then try sending
    if node.node_id in topo:
        for neigh in topo.get(node.node_id):
            await node.log("Sending {} to neigh: {}".format(message, neigh))
            resp = await node.rpc(neigh, {"type": "gossip3b", "message": message})
            await node.log("Received response: ", resp)


@node.handler
async def gossip3b(req: Request) -> Body:
    message = req.body["message"]
    global messages
    share = False
    await node.log("Received gossip from sender: {}, message: {}".format(req.src, message))
    if message not in messages: # if its not already present, save and share with neighbours
        messages.add(message)
        share = True
        await share_with_neigh(message)
    # Do not share with neighbours again here multiple times since it might lead to infinite loop
    # Each node should take care of updating its neighbour only once
    # Check if the message is already present in set, if its preset then don't send
    # We can try keeping a message count and do it later and check
    return {"type": "gossip3b_ok", "share": share}  # Is this response necessary? It just also used in rpc output we can get and use it


node.run()

# ./maelstrom/maelstrom test -w broadcast --bin broadcast_3b_optim.py --node-count 5 --time-limit 20 --rate 10

# ./maelstrom/maelstrom serve

# http://localhost:8080/
