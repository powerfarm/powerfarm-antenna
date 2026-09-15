"""Instantiate the shipped service templates using the existing Powerfarm CLI.

Run as the authenticated lab operator. Credentials are stored only in the supplied
private directory; every Registry mutation goes through that operator's CLI/RLS.
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--cli",required=True)
    p.add_argument("--node",required=True)
    p.add_argument("--source",required=True)
    p.add_argument("--commit",required=True)
    p.add_argument("--private",required=True)
    p.add_argument("--source-repo",required=True)
    a=p.parse_args()
    private=Path(a.private);private.mkdir(parents=True,exist_ok=True,mode=0o700)
    def cli(*argv):
        r=subprocess.run([a.node,a.cli,*argv,"--json"],capture_output=True,text=True)
        if r.returncode:
            raise RuntimeError("CLI "+" ".join(argv[:3])+": "+r.stderr)
        return json.loads(r.stdout)
    until=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=90)).isoformat()
    installed=[]
    for transport in ["http","webhook","websocket","sse"]:
        slug="pf.antenna."+transport
        cli("register","workflow","--slug",slug,"--title","Antenna "+transport,
            "--owner","pf.danvoulez","--lifecycle","production","--metadata",json.dumps({
                "trigger":{"kind":"webhook" if transport=="webhook" else "event","transport":transport},"steps":["preserve","inspect"]}))
        artifact="pf.service.antenna."+transport
        cli("service","publish","--file",str(Path(a.source)/"contracts"/(transport+".json")),
            "--artifact",artifact,"--version","1","--repo",a.source_repo,"--commit",a.commit,"--path","contracts/"+transport+".json")
        parent="antenna."+transport
        c=cli("service","create",parent,"--template",artifact+"@1","--provider","pf.antenna","--client",slug,
            "--until",until)
        for party in ["pf.antenna",slug]: cli("service","accept",parent,"--sha256",c["terms_sha256"],"--party",party)
        name="powerfarm-cli."+transport
        child=cli("service","client",name,"--service",parent,"--client","pf.powerfarm-cli","--until",until)
        for party in [slug,"pf.powerfarm-cli"]: cli("service","accept",name,"--sha256",child["terms_sha256"],"--party",party)
        installed.append({"service":parent,"client":name,"contract_id":child["id"],"sha256":child["terms_sha256"]})
        print(json.dumps(installed[-1]),flush=True)
    for entity,label,filename in [("pf.antenna","Antenna runtime contract snapshots","antenna-registry.token"),
                                  ("pf.powerfarm-cli","Antenna contracted services client","antenna-client.token")]:
        path=private/filename
        if not path.exists():
            receipt=cli("service","credential","--entity",entity,"--label",label,"--out",str(path))
            print(json.dumps(receipt),flush=True)
    (private/"service-activation.json").write_text(json.dumps(installed,indent=2)+"\n")


if __name__=="__main__":main()
