"""Exercise the installed Antenna service contracts over their actual transports."""
import argparse
import asyncio
import json
from pathlib import Path
import urllib.request
import websockets


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--url",required=True)
    p.add_argument("--token-file",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    token=Path(a.token_file).read_text().strip()
    base=a.url.rstrip('/')
    results=[]
    def request(body,contract=None,accept=None):
        headers={"Content-Type":"application/json","Authorization":"Bearer "+token,"User-Agent":"antenna-service-verifier/1"}
        if contract:headers["Antenna-Contract"]=contract
        if accept:headers["Accept"]=accept
        if body.get("jsonrpc"):
            headers.update({"Mcp-Protocol-Version":"2026-07-28","Mcp-Method":body["method"],"Mcp-Name":body["params"]["name"]})
        req=urllib.request.Request(base+'/',data=json.dumps(body).encode(),headers=headers)
        with urllib.request.urlopen(req,timeout=65) as response:
            data=response.read().decode()
            if accept:
                return json.loads(next(line[6:] for line in data.splitlines() if line.startswith('data: ')))['response']
            return json.loads(data)
    payload={"event":"antenna.service.activation","source":"powerfarm-cli","message":"O trabalho entra, o grafo executa, o resultado permanece."}
    for transport in ['http','webhook','sse']:
        reply=request({**payload,"transport":transport},'powerfarm-cli.'+transport,'text/event-stream' if transport=='sse' else None)
        assert reply['status']=='completed',reply
        assert reply['result']['capability']=='service.invoke',reply
        results.append({"transport":transport,"receipt_id":reply['receipt_id'],"contract_id":reply['result']['contract_id'],"result":reply['result']})
        print(json.dumps({"transport":transport,"receipt_id":reply['receipt_id'],"status":reply['status']}),flush=True)
    async def websocket():
        uri=base.replace('https://','wss://',1).replace('http://','ws://',1)+'/'
        async with websockets.connect(uri,additional_headers={"Authorization":"Bearer "+token,"Antenna-Contract":"powerfarm-cli.websocket"},user_agent_header="antenna-service-verifier/1") as ws:
            await ws.send(json.dumps({**payload,"id":"activation-websocket","transport":"websocket"}))
            reply=json.loads(await asyncio.wait_for(ws.recv(),30))
            assert reply['status']=='completed' and reply['id']=='activation-websocket',reply
            results.append({"transport":"websocket","receipt_id":reply['receipt_id'],"result":reply['result']})
            print(json.dumps({"transport":"websocket","receipt_id":reply['receipt_id'],"status":reply['status']}),flush=True)
    asyncio.run(websocket())
    def mcp(arguments):
        return request({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"invoke_service","arguments":arguments,"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}})
    reply=mcp({"contract":"powerfarm-cli.http","input":{**payload,"transport":"mcp"}})
    assert reply['result']['isError'] is False,reply
    rid=reply['result']['_meta']['antenna/receipt_id']
    inspected=mcp({"contract":"powerfarm-cli.http","receipt_id":rid})
    assert inspected['result']['isError'] is False,inspected
    assert inspected['result']['structuredContent']['result']['receipt_id']==rid,inspected
    assert inspected['result']['structuredContent']['result']['status']=='routed',inspected
    results.append({"transport":"mcp","receipt_id":rid,"response":reply,"inspection":inspected})
    print(json.dumps({"transport":"mcp","receipt_id":rid,"inspection":"routed"}),flush=True)
    Path(a.out).write_text(json.dumps({"url":base,"results":results},indent=2,ensure_ascii=False)+'\n')


if __name__=='__main__':main()
