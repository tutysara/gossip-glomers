#!/usr/bin/env python3

import asyncio
import random
from maelstrom import Error, Node, Body, Request

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
    message = req.body["message"]
    msgId = req.body["msg_id"]
    await node.log("*** Received broadcast from sender: {}, message: {} and msgId: {} ***".format(req.src, message, msgId))
    global messages
    messages.add(message)
    # Assert failed: Invalid dest for message #maelstrom.net.message.Message{:id 626, :src "n1", :dest "c21", :body {:type "broadcast_ok", :in_reply_to 4}}
    # Run it in separete loop parallel (concurrent) to the main broadcast
    asyncio.create_task(share_with_neigh(message))  # execute in diff event loop
    await node.log("*** Respond to broadcast from sender: {}, message: {} and msgId: {} ***".format(req.src, message, msgId))

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
    # check if topo is initialised and then try sending
    if node.node_id in topo:
        neighs_to_send = [(neigh,1) for neigh in topo.get(node.node_id)]
        # send to only 30% of neighbours
        to_take = max(len(neighs_to_send)//3, 2)
        if len(neighs_to_send) > to_take:
            neighs_to_send = neighs_to_send[:to_take]

        while len(neighs_to_send) > 0:
            to_retry = []
            for neigh, send_count in neighs_to_send:
                await node.log("--- Sharing {} to neigh: {} times: {} ---".format(message, neigh, send_count))
                # retry with exponential delay and jitter
                await asyncio.sleep(random.randint(0, send_count)/random.randint(1, 10)) # float between 0 to send_count sec
                resp = await node.rpc(neigh, {"type": "gossip3c", "message": message})
                await node.log("Received response: ", resp)
                if resp["type"] == "error" and resp["code"] == Error.TIMEOUT:
                    to_retry.append((neigh, send_count+1))
            await node.log("Failed neighbours =", to_retry)
            neighs_to_send = to_retry   


@node.handler
async def gossip3c(req: Request) -> Body:
    message = req.body["message"]
    global messages
    await node.log("Received gossip from sender: {}, message: {}".format(req.src, message))
    if message not in messages:  # if its not already present, save and share with neighbours
        messages.add(message)
        # it also works if this is done in same loop
        # throughput was 89 in same loop and when running concurrently got 96 throughput
        asyncio.create_task(share_with_neigh(message)) # run it asyncly or concurrently in another loop
    # Do not share with neighbours again here multiple times since it might lead to infinite loop
    # Each node should take care of updating its neighbour only once
    # Check if the message is already present in set, if its preset then don't send
    # We can try keeping a message count and do it later and check
    return {"type": "gossip3c_ok"}  # Is this response necessary? It just also used in rpc output we can get and use it


node.run()

# ./maelstrom/maelstrom test -w broadcast --bin broadcast_3e.py --node-count 25 --time-limit 20 --rate 100 --latency 100
# ./maelstrom/maelstrom serve

# http://localhost:8080/
