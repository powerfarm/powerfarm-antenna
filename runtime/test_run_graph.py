import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

class GraphTests(unittest.TestCase):
    def test_conditional_edge_selects_only_the_requested_successor(self):
        from run_graph import advance
        with tempfile.TemporaryDirectory() as directory:
            request={"checkpoint":str(Path(directory)/"state.db"),"receipt_id":"branch-test","input":{"kind":"keep","content":"branch"},
                "capabilities":["object.store","document.inspect"],"graph":{"start":"route","nodes":{"route":"object.store","inspect":"document.inspect"},
                    "edges":[["inspect","END"]],"branches":{"route":{"on":"input.kind","routes":{"keep":"inspect","stop":"END"}}}}}
            self.assertEqual(advance(request)["node"],"route")
            request["resume"]={"node":"route","result":{"digest":"abc"}}
            self.assertEqual(advance(request)["node"],"inspect")
    def test_checkpoint_survives_separate_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            request = {"checkpoint":str(Path(directory)/"state.db"),"receipt_id":"rcp_test",
                "input":{"content":"persist this"},"capabilities":["object.store","document.inspect"],
                "graph":{"start":"store","nodes":{"store":"object.store","inspect":"document.inspect"},
                    "edges":[["store","inspect"],["inspect","END"]],
                    "parameters":{"inspect":{"digest":{"from":"result.digest"}}}}}
            def advance(value):
                p=subprocess.run([sys.executable,str(Path(__file__).with_name("run_graph.py"))],input=json.dumps(value),text=True,capture_output=True)
                self.assertEqual(p.returncode,0,p.stdout+p.stderr)
                return json.loads(p.stdout)
            first=advance(request)
            self.assertEqual(first["capability"],"object.store")
            self.assertEqual(advance(request),first,"a new process sees the same suspended node")
            resumed={**request,"resume":{"node":"store","result":{"digest":"abc"},"deliveries":[]}}
            second=advance(resumed)
            self.assertEqual(second["capability"],"document.inspect")
            self.assertEqual(second["params"],{"digest":"abc"})
            self.assertEqual(advance(request),second,"completed store is not repeated")
            done=advance({**request,"resume":{"node":"inspect","result":{"size":12},"deliveries":[]}})
            self.assertEqual(done["status"],"completed")
            self.assertEqual(advance(request),done,"completed graph remains completed after restart")

if __name__=="__main__": unittest.main()
