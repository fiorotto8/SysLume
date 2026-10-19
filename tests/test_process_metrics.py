"""Tests for collectors.process_metrics using mocked /proc and subprocess."""

import json
import os
import tempfile
import time
import unittest
import unittest.mock

from collectors import process_metrics as pm

HZ = 100
BTIME = 1_700_000_000

OLLAMA_CGROUP = "0::/system.slice/ollama.service"
DOCKERCGR = "0::/docker/" + "a" * 64 + "/scope"


class FakeRun:
    """Stands in for process_metrics._run; records every invocation."""

    def __init__(self, behaviors):
        self.behaviors = behaviors  # argv[0] -> (code, out, err, kind)
        self.calls = []

    def __call__(self, args, timeout, _runner=None):
        self.calls.append((list(args), timeout))
        behavior = self.behaviors.get(args[0])
        if behavior is None:
            return None, "", "", "missing"
        return behavior


def write_pid(root, spec):
    """Create one fake /proc/<pid> tree from a spec dict."""
    pdir = os.path.join(root, str(spec["pid"]))
    os.makedirs(pdir, exist_ok=True)
    if spec.get("unreadable"):
        return  # no comm file -> OSError on read
    with open(os.path.join(pdir, "comm"), "w") as fh:
        fh.write(spec.get("comm", "proc") + "\n")
    argv = spec.get("argv") or (["/bin/" + spec.get("comm", "proc"),
                                 "--api-key", None])
    argv = [a if a is not None else ("arg%d" % i)
            for i, a in enumerate(argv, 1)]
    with open(os.path.join(pdir, "cmdline"), "wb") as fh:
        fh.write(b"\0".join(a.encode() for a in argv))
    with open(os.path.join(pdir, "cgroup"), "w") as fh:
        fh.write(spec.get("cgroup", "0::/init.scope\n"))
    if spec.get("starttime") is not None:
        fields = ["0"] * 27
        fields[18] = str(spec["starttime"])  # field 22 = 'S'+idx18 after ')'
        with open(os.path.join(pdir, "stat"), "w") as fh:
            fh.write("%d (%s) S %s\n" % (spec["pid"], spec.get("comm", "proc"),
                                         " ".join(fields)))


def build_proc_root(pids):
    """Return (proc_root, cleanup) with the given pid specs."""
    root = tempfile.TemporaryDirectory()
    proot = os.path.join(root.name, "proc")
    os.makedirs(proot)
    with open(os.path.join(proot, "stat"), "w") as fh:
        fh.write("btime %d\n" % BTIME)
    for spec in pids:
        write_pid(proot, spec)
    return proot, root.cleanup


def recent_starttime(seconds_ago):
    return int((time.time() - BTIME - seconds_ago) * HZ)


DOCKER_OK_EMPTY = (0, "", "", None)
TMUX_NO_SERVER = (1, "", "no server running on /tmp/tmux-1000/default",
                  None)
TMUX_CONNECT_ERR = (
    1, "",
    "error connecting to /tmp/tmux-1000/default (No such file or "
    "directory)", None)
TMUX_RUNNING = (0, "openclaw\nmisc\n", "", None)
DOCKER_ONE_NODE = (0, "node-0\tghcr.io/vllm/cluster:1.0\n", "", None)
DOCKER_ONE_NODE1 = (0, "node-1\tghcr.io/vllm/cluster:1.0\n", "", None)


class Dgx01SplitLayoutTests(unittest.TestCase):
    """DGX01: Ollama + two local Q8 runners, no heavy stack."""

    def setUp(self):
        self.pids = [
            {"pid": 1, "comm": "systemd", "cgroup": "0::/init.scope",
             "starttime": 1000},
            {"pid": 2, "comm": "kworker/0:1", "argv": [],
             "cgroup": "0::"},
            {"pid": 1001, "comm": "ollama", "argv": ["ollama", "serve"],
             "cgroup": OLLAMA_CGROUP, "starttime": 5000},
            {"pid": 1002, "comm": "llama-server",
             "argv": ["/usr/local/lib/ollama/llama-server", "--model",
                      "/usr/share/ollama/models/blobs/sha256-aaa"],
             "cgroup": OLLAMA_CGROUP,
             "starttime": recent_starttime(1000)},
            {"pid": 1003, "comm": "llama-server",
             "argv": ["/usr/local/lib/ollama/llama-server", "--model",
                      "/usr/share/ollama/models/blobs/sha256-bbb"],
             "cgroup": OLLAMA_CGROUP,
             "starttime": recent_starttime(3600)},
            {"pid": 1004, "comm": "node", "argv": ["node", "/opt/openclaw"]},
            {"pid": 1005, "comm": "python3",
             "argv": ["python3", "/opt/pi-agent/pi/session.py"]},
            # earlyoom carries 'vllm'/'ray' in its --prefer regex:
            # neither must be classified as inference processes.
            {"pid": 1006, "comm": "earlyoom",
             "argv": ["/usr/bin/earlyoom", "--prefer",
                      "(^|/)(java|vllm|VLLM|ray|python3)"]},
        ]

    def test_dgx01_split_layout(self):
        root, cleanup = build_proc_root(self.pids)
        try:
            runner = FakeRun({"docker": DOCKER_OK_EMPTY,
                              "tmux": TMUX_CONNECT_ERR})
            s = pm.ProcessMetrics(proc_root=root, runner=runner,
                                  clock_hz=HZ).collect()
            m = s["metrics"]
            self.assertEqual(s["status"], "ok")
            self.assertIsNone(s["error"])
            self.assertEqual(m["ollama_daemon_count"], 1)
            self.assertEqual(m["ollama_runner_count"], 2)
            self.assertEqual(m["ray_process_count"], 0)
            self.assertEqual(m["vllm_process_count"], 0)
            self.assertEqual(m["vllm_engine_core_count"], 0)
            self.assertEqual(m["vllm_rank_worker_count"], 0)
            self.assertEqual(m["docker_container_count"], 0)
            self.assertEqual(m["tmux_session_count"], 0)
            d = s["details"]
            self.assertEqual(d["ollama"]["daemon_pids"], [1001])
            self.assertEqual(d["ollama"]["runner_pids"], [1002, 1003])
            self.assertEqual(len(d["ollama"]["runners"]), 2)
            up = {r["pid"]: r["uptime_seconds"] for r in d["ollama"]["runners"]}
            self.assertGreater(up[1002], 900)
            self.assertLess(up[1002], 1100)
            self.assertGreater(up[1003], 3500)
            self.assertLess(up[1003], 3700)
            self.assertFalse(d["ray"]["present"])
            self.assertFalse(d["vllm"]["present"])
            self.assertTrue(d["docker"]["reachable"])
            self.assertEqual(d["docker"]["node_container_names"], [])
            self.assertIs(d["tmux"]["server_running"], False)
            self.assertEqual(d["proc"]["unreadable_pids"], 0)
        finally:
            cleanup()


class Dgx02SplitLayoutTests(unittest.TestCase):
    def test_dgx02_split_layout(self):
        pids = [
            {"pid": 1001, "comm": "ollama", "argv": ["ollama", "serve"],
             "cgroup": OLLAMA_CGROUP},
            {"pid": 1002, "comm": "llama-server",
             "argv": ["/usr/local/lib/ollama/llama-server", "--model",
                      "/blobs/qwen3.8-27b"], "cgroup": OLLAMA_CGROUP,
             "starttime": recent_starttime(500)},
        ]
        root, cleanup = build_proc_root(pids)
        try:
            runner = FakeRun({"docker": DOCKER_OK_EMPTY,
                              "tmux": TMUX_RUNNING})
            s = pm.ProcessMetrics(proc_root=root, runner=runner,
                                  clock_hz=HZ).collect()
            m = s["metrics"]
            self.assertEqual(s["status"], "ok")
            self.assertEqual(m["ollama_daemon_count"], 1)
            self.assertEqual(m["ollama_runner_count"], 1)
            self.assertEqual(m["ray_process_count"], 0)   # expected in split
            self.assertEqual(m["vllm_process_count"], 0)  # expected in split
            self.assertEqual(m["docker_container_count"], 0)
            self.assertEqual(m["tmux_session_count"], 2)
            d = s["details"]
            self.assertIs(d["ray"]["present"], False)
            self.assertIs(d["vllm"]["present"], False)
            self.assertIs(d["tmux"]["server_running"], True)
            self.assertEqual(d["tmux"]["relevant_sessions"], [])
            self.assertFalse(s["warnings"])
        finally:
            cleanup()


class Dgx02HeavyLayoutTests(unittest.TestCase):
    """DGX02 heavy: Ray head + vLLM rank 0 (container), Ollama stopped."""

    def setUp(self):
        self.pids = [
            # Ollama deliberately absent (stopped for heavy mode).
            {"pid": 2001, "comm": "raylet",
             "argv": ["/usr/local/bin/raylet", "--raylet_params=redacted"],
             "cgroup": "0::/system.slice/ray.service"},
            {"pid": 2002, "comm": "gcs_server",
             "argv": ["/usr/local/bin/gcs_server"]},
            {"pid": 2003, "comm": "ray::IDLE", "argv": ["ray::IDLE",
                "/usr/local/lib/python3.12/dist-packages/ray/workers/"
                "default_worker.py"]},
            {"pid": 2004, "comm": "python3",
             "argv": ["python3", "-m", "vllm.entrypoints.openai.api_server",
                      "--model", "heavy-qwen"],
             "cgroup": DOCKERCGR},
            {"pid": 2005, "comm": "VLLM::EngineCore",
             "argv": ["VLLM::EngineCore"]},
            {"pid": 2006, "comm": "Worker",
             "argv": ["python3", "-u",
                      "/usr/local/lib/python3.12/dist-packages/vllm/worker/"
                      "worker.py"]},
        ]

    def test_dgx02_heavy_layout(self):
        root, cleanup = build_proc_root(self.pids)
        try:
            runner = FakeRun({"docker": DOCKER_ONE_NODE,
                              "tmux": (0, "heavy-qwen\n", "", None)})
            s = pm.ProcessMetrics(proc_root=root, runner=runner,
                                  clock_hz=HZ).collect()
            m = s["metrics"]
            self.assertEqual(s["status"], "ok")          # not unhealthy
            self.assertEqual(m["ollama_daemon_count"], 0)  # stopped: ok
            self.assertEqual(m["ollama_runner_count"], 0)
            self.assertEqual(m["ray_process_count"], 3)
            self.assertEqual(m["vllm_process_count"], 3)
            self.assertEqual(m["vllm_engine_core_count"], 1)
            self.assertEqual(m["vllm_rank_worker_count"], 1)
            self.assertEqual(m["docker_container_count"], 1)
            self.assertEqual(m["tmux_session_count"], 1)
            d = s["details"]
            self.assertEqual(d["vllm"]["pids"], [2004, 2005, 2006])
            self.assertEqual(d["vllm"]["container_split"],
                             {"host": 2, "container": 1, "unknown": 0})
            self.assertEqual(d["ray"]["pids"], [2001, 2002, 2003])
            self.assertEqual(d["docker"]["node_container_names"], ["node-0"])
            self.assertEqual(d["tmux"]["relevant_sessions"], ["heavy-qwen"])
        finally:
            cleanup()


class Dgx03HeavyWorkerLayoutTests(unittest.TestCase):
    """DGX03 heavy: Ray worker + vLLM rank 1 in container, no API server."""

    def test_dgx03_heavy_worker_layout(self):
        pids = [
            # Ollama stopped on DGX03 in heavy mode.
            {"pid": 3001, "comm": "ray::IDLE", "argv": ["ray::IDLE",
                "/usr/local/lib/python3.12/dist-packages/ray/workers/"
                "default_worker.py"], "cgroup": DOCKERCGR},
            {"pid": 3002, "comm": "EngineCore_DP0", "argv": [],
             "cgroup": DOCKERCGR},
            {"pid": 3003, "comm": "Worker", "argv": ["python3", "-u",
                "/usr/local/lib/python3.12/dist-packages/vllm/worker/"
                "worker.py"], "cgroup": DOCKERCGR},
        ]
        root, cleanup = build_proc_root(pids)
        try:
            runner = FakeRun({"docker": DOCKER_ONE_NODE1,
                              "tmux": (0, "heavy-qwen-worker\n", "", None)})
            s = pm.ProcessMetrics(proc_root=root, runner=runner,
                                  clock_hz=HZ).collect()
            m = s["metrics"]
            self.assertEqual(s["status"], "ok")
            self.assertIsNone(s["error"])
            self.assertEqual(m["ollama_daemon_count"], 0)
            self.assertEqual(m["ray_process_count"], 1)   # worker, no head
            self.assertEqual(m["vllm_process_count"], 2)
            self.assertEqual(m["vllm_engine_core_count"], 1)
            self.assertEqual(m["vllm_rank_worker_count"], 1)
            self.assertEqual(m["docker_container_count"], 1)
            d = s["details"]
            # No vLLM API process on the rank-1 node is NOT unhealthy.
            self.assertEqual(d["vllm"]["container_split"],
                             {"host": 0, "container": 2, "unknown": 0})
            self.assertEqual(d["docker"]["node_container_names"], ["node-1"])
            self.assertEqual(d["proc"]["container_split"]["container"], 3)
            self.assertEqual(d["tmux"]["relevant_sessions"],
                             ["heavy-qwen-worker"])
        finally:
            cleanup()

    def test_runner_uptime_unavailable_is_null(self):
        pids = [
            {"pid": 4002, "comm": "llama-server",
             "argv": ["/usr/local/lib/ollama/llama-server", "--model",
                      "/blobs/x"],  # no stat file -> uptime unknown
        }
        ]
        root, cleanup = build_proc_root(pids)
        try:
            s = pm.ProcessMetrics(proc_root=root,
                                  runner=FakeRun({}), clock_hz=HZ).collect()
            runners = s["details"]["ollama"]["runners"]
            self.assertEqual(len(runners), 1)
            self.assertIsNone(runners[0]["uptime_seconds"])
            # docker/tmux binaries absent: unknown, not zero
            self.assertIsNone(s["metrics"]["docker_container_count"])
            self.assertIsNone(s["metrics"]["tmux_session_count"])
        finally:
            cleanup()


class DockerFailureTests(unittest.TestCase):
    def setUp(self):
        pids = [{"pid": 1001, "comm": "ollama", "argv": ["ollama"]}]
        root, cleanup = build_proc_root(pids)
        self.addCleanup(cleanup)
        self.root = root

    def _collect(self, docker_behavior):
        return pm.ProcessMetrics(
            proc_root=self.root,
            runner=FakeRun({"docker": docker_behavior,
                            "tmux": TMUX_NO_SERVER}),
            clock_hz=HZ).collect()

    def test_docker_binary_missing(self):
        s = self._collect((None, "", "", "missing"))
        self.assertIsNone(s["metrics"]["docker_container_count"])
        self.assertIs(s["details"]["docker"]["reachable"], False)
        self.assertEqual(s["status"], "ok")

    def test_docker_permission_denied(self):
        s = self._collect((1, "", "permission denied while trying to "
                                 "connect to the Docker daemon", None))
        self.assertIsNone(s["metrics"]["docker_container_count"])
        self.assertIs(s["details"]["docker"]["reachable"], False)
        self.assertEqual(s["details"]["docker"]["problem"],
                         "permission denied")
        self.assertEqual(s["status"], "ok")

    def test_docker_timeout(self):
        s = self._collect((None, "", "", "timeout"))
        self.assertIsNone(s["metrics"]["docker_container_count"])
        self.assertIsNone(s["details"]["docker"]["reachable"])
        self.assertIn("docker", s["warnings"][0])
        self.assertEqual(s["status"], "ok")


class UnreadableProcTests(unittest.TestCase):
    def test_unreadable_pid_makes_counts_null_not_zero(self):
        pids = [
            {"pid": 1001, "comm": "ollama", "argv": ["ollama"]},
            {"pid": 5555, "unreadable": True},
        ]
        root, cleanup = build_proc_root(pids)
        try:
            s = pm.ProcessMetrics(proc_root=root, runner=FakeRun({
                "docker": DOCKER_OK_EMPTY, "tmux": TMUX_NO_SERVER}),
                clock_hz=HZ).collect()
            m = s["metrics"]
            self.assertEqual(s["status"], "ok")
            for key in ("ollama_daemon_count", "ollama_runner_count",
                        "ray_process_count", "vllm_process_count",
                        "vllm_engine_core_count", "vllm_rank_worker_count"):
                self.assertIsNone(m[key], key)
            self.assertEqual(m["docker_container_count"], 0)  # not affected
            self.assertFalse(s["details"]["proc"]["complete"])
            self.assertEqual(s["details"]["proc"]["unreadable_pids"], 1)
            self.assertIsNone(s["details"]["ollama"]["runners"])
            self.assertIsNone(s["details"]["ray"]["present"])
            self.assertTrue(any("unreadable" in w for w in s["warnings"]))
        finally:
            cleanup()

    def test_unreadable_proc_root_is_error(self):
        s = pm.ProcessMetrics(proc_root="/nonexistent/proc-root",
                              runner=FakeRun({}), clock_hz=HZ).collect()
        self.assertEqual(s["status"], "error")
        self.assertIsNotNone(s["error"])
        self.assertIsNone(s["metrics"]["ollama_daemon_count"])
        self.assertIsNone(s["details"]["proc"])


class VanishingProcTests(unittest.TestCase):
    def test_process_vanishes_during_sampling(self):
        pids = [
            {"pid": 1001, "comm": "ollama", "argv": ["ollama"]},
            {"pid": 999, "comm": "llama-server",
             "argv": ["/llama-server"]},
        ]
        root, cleanup = build_proc_root(pids)
        try:
            original = pm._read_comm

            def flaky(pdir):
                if pdir.endswith("/999"):
                    return None, True  # ESRCH: gone mid-sample
                return original(pdir)

            with unittest.mock.patch.object(pm, "_read_comm", flaky):
                s = pm.ProcessMetrics(proc_root=root,
                                      runner=FakeRun({
                                          "docker": DOCKER_OK_EMPTY,
                                          "tmux": TMUX_NO_SERVER}),
                                      clock_hz=HZ).collect()
            self.assertEqual(s["status"], "ok")
            self.assertEqual(s["metrics"]["ollama_daemon_count"], 1)
            self.assertEqual(s["metrics"]["ollama_runner_count"], 0)
            self.assertTrue(s["details"]["proc"]["complete"])
            self.assertEqual(
                s["details"]["proc"]["vanished_mid_sample"], 1)
        finally:
            cleanup()


class TmuxBehaviorTests(unittest.TestCase):
    def test_no_server_is_zero_not_error(self):
        root, cleanup = build_proc_root([
            {"pid": 1001, "comm": "ollama", "argv": ["ollama"]}])
        try:
            s = pm.ProcessMetrics(proc_root=root, runner=FakeRun({
                "docker": DOCKER_OK_EMPTY, "tmux": TMUX_NO_SERVER}),
                clock_hz=HZ).collect()
            self.assertEqual(s["metrics"]["tmux_session_count"], 0)
            self.assertIs(s["details"]["tmux"]["server_running"], False)
            self.assertIsNone(s["details"]["tmux"]["problem"])
        finally:
            cleanup()

    def test_error_connecting_variant_is_zero(self):
        root, cleanup = build_proc_root([])
        try:
            s = pm.ProcessMetrics(proc_root=root, runner=FakeRun({
                "tmux": TMUX_CONNECT_ERR}), clock_hz=HZ).collect()
            self.assertEqual(s["metrics"]["tmux_session_count"], 0)
            self.assertIs(s["details"]["tmux"]["server_running"], False)
        finally:
            cleanup()

    def test_tmux_binary_missing_is_unknown(self):
        root, cleanup = build_proc_root([])
        try:
            s = pm.ProcessMetrics(proc_root=root, runner=FakeRun({}),
                                  clock_hz=HZ).collect()
            self.assertIsNone(s["metrics"]["tmux_session_count"])
            self.assertIsNone(s["details"]["tmux"]["server_running"])
        finally:
            cleanup()


class SanitizationTests(unittest.TestCase):
    def test_no_secrets_or_full_cmdlines_in_output(self):
        secret1 = "sk-supersecret123"
        secret2 = "Bearer token-abc-987"
        pids = [
            {"pid": 1001, "comm": "ollama",
             "argv": ["ollama", "serve"]},
            {"pid": 1002, "comm": "llama-server",
             "argv": ["/usr/local/lib/ollama/llama-server",
                      "--api-key", secret1, "--url", "https://x",
                      secret2, "--model", "/blobs/xyz"]},
            {"pid": 2004, "comm": "python3",
             "argv": ["python3", "-m", "vllm.entrypoints.openai.api_server",
                      "--env", "KEY=" + secret1]},
        ]
        root, cleanup = build_proc_root(pids)
        try:
            s = pm.ProcessMetrics(proc_root=root, runner=FakeRun({
                "docker": DOCKER_OK_EMPTY, "tmux": TMUX_NO_SERVER}),
                clock_hz=HZ).collect()
            blob = json.dumps(s)
            self.assertNotIn(secret1, blob)
            self.assertNotIn(secret2, blob)
            details_blob = json.dumps(s["details"])
            details_keys = json.dumps(list(s["details"].keys()) +
                                      list(s["details"]["ollama"].keys()))
            self.assertNotIn("cmdline", details_keys)  # no cmdline fields
            # argv text must not leak, but pids must be retained:
            self.assertNotIn("api_server", details_blob)
            self.assertNotIn("--api-key", details_blob)
            self.assertIn(1002, s["details"]["ollama"]["runner_pids"])
        finally:
            cleanup()

    def test_docker_stderr_truncated_and_no_full_output(self):
        big = "x" * 20000
        root, cleanup = build_proc_root([])
        try:
            s = pm.ProcessMetrics(proc_root=root,
                                  runner=FakeRun({
                                      "docker": (1, "", big, None),
                                      "tmux": TMUX_NO_SERVER}),
                                  clock_hz=HZ).collect()
            blob = json.dumps(s)
            self.assertLess(len(blob), 50000)
            self.assertTrue(len(s["details"]["docker"]["problem"]) < 300)
        finally:
            cleanup()


class ShapeAndSafetyTests(unittest.TestCase):
    def _sample(self):
        pids = [
            {"pid": 1001, "comm": "ollama", "argv": ["ollama"]},
            {"pid": 1002, "comm": "llama-server",
             "argv": ["/llama-server"], "starttime": recent_starttime(10)},
            {"pid": 2003, "comm": "ray::IDLE", "argv": ["ray::IDLE"]},
        ]
        root, cleanup = build_proc_root(pids)
        self.addCleanup(cleanup)
        runner = FakeRun({"docker": DOCKER_OK_EMPTY,
                          "tmux": TMUX_NO_SERVER})
        return pm.ProcessMetrics(proc_root=root, runner=runner,
                                 clock_hz=HZ).collect(), runner

    def test_metrics_numeric_or_null(self):
        s, _ = self._sample()
        for key, value in s["metrics"].items():
            self.assertIsInstance(value, (int, type(None)), key)
            if value is not None:
                self.assertFalse(isinstance(value, bool), key)
        self.assertIsInstance(s["timestamp"], float)
        self.assertIsInstance(s["status"], str)

    def test_json_serialization_roundtrip(self):
        s, _ = self._sample()
        clone = json.loads(json.dumps(s))
        self.assertEqual(clone["metrics"], s["metrics"])
        self.assertEqual(clone["details"], s["details"])

    def test_no_state_changing_commands_executed(self):
        s, runner = self._sample()
        self.assertEqual(s["status"], "ok")
        self.assertEqual(len(runner.calls), 2)  # docker once, tmux once
        for args, timeout in runner.calls:
            self.assertIn(args[0], ("docker", "tmux"))
            self.assertLess(timeout, 30)
            flat = [a.lower() for a in args]
            for forbidden in ("exec", "stop", "start", "kill", "systemctl",
                              "sudo", "rm", "restart", "down"):
                self.assertNotIn(forbidden, flat)
        expected = {
            ("docker",): ["docker", "ps", "--format",
                          "{{.Names}}\t{{.Image}}"],
            ("tmux",): ["tmux", "list-sessions", "-F", "#{session_name}"],
        }
        for args, _ in runner.calls:
            self.assertEqual(args, expected[(args[0],)])

    def test_refuses_unauthorized_command(self):
        with self.assertRaises(ValueError):
            pm._run(["rm", "-rf", "/"], 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
