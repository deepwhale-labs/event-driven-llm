import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from codex_bridge import CodexRunner, ROOT, make_server


class BridgeTest(unittest.TestCase):
    def setUp(self):
        build = (ROOT / 'build').resolve()
        build.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix='cli-bridge-test-', dir=build)
        self.folder = Path(self.temporary.name).resolve()
        assert self.folder.is_relative_to(build)
        self.addCleanup(self.temporary.cleanup)
        self.config = dict(token='x' * 64, model='test-model', port=0, timeout_seconds=5, reasoning_effort='medium')
        stub = self.folder / 'fake_cli.py'
        stub.write_text('''import json, os, sys, time
from pathlib import Path
args = sys.argv[1:]
prompt = sys.stdin.buffer.read().decode('utf-8')
Path('arguments.json').write_text(json.dumps({'args': args, 'prompt': prompt, 'api_key_present': 'OPENAI_API_KEY' in os.environ}), encoding='utf-8')
if prompt == 'timeout': time.sleep(10)
answer = Path(args[args.index('--output-last-message') + 1])
answer.write_text('' if prompt == 'empty' else prompt, encoding='utf-8')
if prompt != 'incomplete': print(json.dumps({'type': 'turn.completed'}))
if prompt == 'fail': sys.exit(2)
''', encoding='utf-8')
        self.runner = CodexRunner(self.config, self.folder / 'runs', [sys.executable, str(stub)])

    def test_unicode_and_shell_metacharacters_remain_data_and_roles_stay_separate(self):
        prompt = '한글 `echo injected` $(whoami) "quoted"; input'
        with patch.dict('os.environ', {'OPENAI_API_KEY': 'not-a-real-key'}):
            self.assertEqual(self.runner.generate('역할 지시', prompt), prompt)
        record = json.loads(next((self.folder / 'runs').glob('*/arguments.json')).read_text(encoding='utf-8'))
        self.assertEqual(record['prompt'], prompt)
        self.assertFalse(record['api_key_present'])
        args = record['args']
        self.assertIn('read-only', args)
        self.assertIn('--ignore-user-config', args)
        self.assertIn('forced_login_method="chatgpt"', args)
        developer = next(arg for arg in args if arg.startswith('developer_instructions='))
        self.assertIn('역할 지시', developer)
        self.assertNotIn(prompt, developer)

    def test_failed_incomplete_and_empty_answers_never_become_success(self):
        for prompt in ('fail', 'incomplete', 'empty'):
            with self.subTest(prompt=prompt), self.assertRaises(RuntimeError):
                self.runner.generate('', prompt)
        self.assertEqual(self.runner.generate('', 'recovered'), 'recovered')

    def test_timeout_terminates_process_and_releases_single_execution_slot(self):
        self.config['timeout_seconds'] = 0.2
        with self.assertRaises(TimeoutError):
            self.runner.generate('', 'timeout')
        self.config['timeout_seconds'] = 5
        self.assertEqual(self.runner.generate('', 'after timeout'), 'after timeout')

    def test_busy_does_not_spawn_another_cli(self):
        self.runner.slot.acquire()
        with self.assertRaises(BlockingIOError):
            self.runner.generate('', 'busy')
        self.runner.slot.release()
        self.assertFalse((self.folder / 'runs').exists())

    def serve(self, runner):
        server = make_server(self.config, runner, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def stop():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        self.addCleanup(stop)
        return 'http://127.0.0.1:' + str(server.server_port)

    def request(self, base, payload=None, token=True, origin=None):
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['X-CLI-Token'] = self.config['token']
        if origin:
            headers['Origin'] = origin
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8') if payload is not None else None
        request = Request(base + ('/generate' if payload is not None else '/health'), data=data, headers=headers)
        try:
            with urlopen(request, timeout=5) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def test_http_auth_validation_and_final_answer_contract(self):
        runner = Mock()
        runner.generate.return_value = '부정'
        base = self.serve(runner)
        valid = dict(model='test-model', system='검토', prompt='고장 났습니다')
        self.assertEqual(self.request(base, valid, token=False)[0], 401)
        self.assertEqual(self.request(base, valid, origin='https://example.com')[0], 401)
        self.assertEqual(self.request(base, {**valid, 'model': 'other'})[0], 409)
        self.assertEqual(self.request(base, {**valid, 'command': 'unsafe'})[0], 400)
        self.assertEqual(self.request(base, {**valid, 'prompt': ' '})[0], 400)
        runner.generate.assert_not_called()
        status, value = self.request(base, valid)
        self.assertEqual((status, value), (200, {'model': 'test-model', 'output': '부정'}))
        runner.generate.assert_called_once_with('검토', '고장 났습니다')
        status, health = self.request(base)
        self.assertEqual(status, 200)
        self.assertNotIn('token', health)

    def test_http_failure_busy_and_timeout_remain_failure(self):
        runner = Mock()
        base = self.serve(runner)
        valid = dict(model='test-model', system='', prompt='hello')
        for error, status in ((RuntimeError('fixture failure'), 502), (BlockingIOError('busy'), 503), (TimeoutError('slow'), 504)):
            runner.generate.side_effect = error
            self.assertEqual(self.request(base, valid)[0], status)


if __name__ == '__main__':
    unittest.main()
