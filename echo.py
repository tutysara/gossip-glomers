#!/usr/bin/env python3

from maelstrom import Node, Body, Request

node = Node()


@node.handler
async def echo(req: Request) -> Body:
    return {"type": "echo_ok", "echo": req.body["echo"]}
    #return {"type": "echo_ok"}



node.run()

# ./maelstrom/maelstrom test -w echo --bin echo.py --node-count 1 --time-limit 10