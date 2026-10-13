"""Timeout retries keep one fenced claim and never publish partial originals."""

from contextlib import redirect_stdout
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests
from urllib3.exceptions import ReadTimeoutError

import unlim_api as api
import unlim_state as queue
import unlim_worker as worker


DATA = b'complete original'


class StreamingResponse:
    status_code = 200
    headers = {'Content-Length': str(len(DATA))}

    def __init__(self, chunks):
        self.chunks = chunks

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def iter_content(self, _):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = os.path.join(self.tmp.name, 'state.db')
        queue.ensure_schema(self.db)
        with queue.transaction(self.db) as c:
            c.execute("INSERT INTO unlim_files(file_id,name) VALUES (?,?)",
                      ('photounlim:/file.jpg', 'file.jpg'))
        self.session = Mock()
        self.session.__enter__ = Mock(return_value=self.session)
        self.session.__exit__ = Mock(return_value=False)
        self.info = {'url': 'https://download.invalid/original', 'size': len(DATA),
                     'md5': hashlib.md5(DATA).hexdigest()}

    def run_worker(self, retries, process=None):
        output = io.StringIO()
        with patch.object(worker, 'make_session', return_value=self.session), \
             patch.object(worker, 'get_sk_uid', return_value=('sk', 'uid')), \
             redirect_stdout(output):
            if process is None:
                with patch.object(worker, 'original_info', return_value=self.info) as info:
                    result = worker.run(1, 'run1', self.tmp.name, self.db,
                                        'unused-cookies', timeout_retries=retries)
                return result, output.getvalue(), info.call_count
            with patch.object(worker, 'process_file', side_effect=process) as called:
                result = worker.run(1, 'run1', self.tmp.name, self.db,
                                    'unused-cookies', timeout_retries=retries)
            return result, output.getvalue(), called.call_count

    def test_wrapped_stream_timeout_retries_with_clean_part_and_same_claim(self):
        wrapped = requests.ConnectionError(
            ReadTimeoutError(None, '/signed?token=private', 'private timeout'))
        self.session.get.side_effect = [
            StreamingResponse([b'partial', wrapped]), StreamingResponse([DATA])]
        result, output, metadata_calls = self.run_worker(2)
        self.assertEqual(result, 0)
        self.assertEqual(metadata_calls, 2)
        self.assertEqual(self.session.get.call_count, 2)
        with queue.connection(self.db) as c:
            row = c.execute('SELECT * FROM unlim_files').fetchone()
        self.assertEqual((row['status'], row['attempts']), ('downloaded', 1))
        self.assertEqual(Path(row['local_path']).read_bytes(), DATA)
        self.assertEqual(list(Path(self.tmp.name).rglob('*.part')), [])
        self.assertIn('повтор 2/3', output)
        self.assertIn('попытка 2/3', output)
        self.assertIn('метаданные', output)
        self.assertIn('передача', output)
        self.assertNotIn('private', output)

    def test_metadata_timeout_refetches_original_before_transfer(self):
        self.session.get.return_value = StreamingResponse([DATA])
        output = io.StringIO()
        with patch.object(worker, 'make_session', return_value=self.session), \
             patch.object(worker, 'get_sk_uid', return_value=('sk', 'uid')), \
             patch.object(worker, 'original_info', side_effect=[
                 api.DownloadError('mpfs/info: ReadTimeout', is_timeout=True),
                 self.info]) as info, redirect_stdout(output):
            result = worker.run(1, 'run1', self.tmp.name, self.db,
                                'unused', timeout_retries=1)
        self.assertEqual(result, 0)
        self.assertEqual(info.call_count, 2)
        self.assertEqual(self.session.get.call_count, 1)
        self.assertIn('фаза=metadata: ошибка', output.getvalue())
        self.assertIn('повтор 2/2', output.getvalue())

    def test_timeout_exhaustion_fails_once_and_reports_every_attempt(self):
        self.session.get.side_effect = [requests.ReadTimeout('secret URL') for _ in range(3)]
        result, output, metadata_calls = self.run_worker(2)
        self.assertEqual(result, 1)
        self.assertEqual(metadata_calls, 3)
        with queue.connection(self.db) as c:
            row = c.execute('SELECT status,attempts,error FROM unlim_files').fetchone()
        self.assertEqual((row['status'], row['attempts'], row['error']),
                         ('failed', 1, 'file: ReadTimeout'))
        self.assertEqual(output.count('ошибка file: ReadTimeout'), 3)
        self.assertIn('отложен до следующего запуска', output)
        self.assertNotIn('secret', output)
        self.assertEqual(list(Path(self.tmp.name).rglob('*.part')), [])

    def test_non_timeout_errors_never_retry(self):
        cases = [
            api.DownloadError('mpfs/info: HTTP 404'),
            api.DownloadError('file: HTTP 500'),
            api.DownloadError('file: HTTP 429', fatal=True, retry_after=10),
            api.DownloadError('file: MD5 не совпадает'),
            OSError(28, 'disk full'),
        ]
        for error in cases:
            with self.subTest(error=type(error).__name__ + str(error)):
                with self.setUpDbAgain():
                    _, output, calls = self.run_worker(3, error)
                    self.assertEqual(calls, 1)
                    with queue.connection(self.db) as c:
                        row = c.execute('SELECT status,attempts FROM unlim_files').fetchone()
                    self.assertEqual((row['status'], row['attempts']), ('failed', 1))
                    self.assertNotIn('повтор 2/4', output)

    def setUpDbAgain(self):
        """Fresh queue for each subtest without touching the user's database."""
        from contextlib import contextmanager
        @contextmanager
        def fresh():
            old = self.db
            self.db = os.path.join(self.tmp.name, os.urandom(5).hex() + '.db')
            queue.ensure_schema(self.db)
            with queue.transaction(self.db) as c:
                c.execute("INSERT INTO unlim_files(file_id,name) VALUES (?,?)",
                          ('photounlim:/file.jpg', 'file.jpg'))
            try:
                yield
            finally:
                self.db = old
        return fresh()

    def test_timeout_types_only_not_exception_text(self):
        wrapped = requests.ConnectionError(
            ReadTimeoutError(None, '/private', 'private timeout'))
        self.assertTrue(api.is_request_timeout(wrapped))
        self.assertTrue(api.is_request_timeout(requests.ConnectTimeout('private')))
        self.assertFalse(api.is_request_timeout(requests.ConnectionError('ReadTimeout private')))

    def test_lost_lease_prevents_second_attempt(self):
        error = api.DownloadError('file: ReadTimeout', is_timeout=True)
        with patch.object(queue, 'renew', return_value=False), \
             patch.object(worker, 'make_session', return_value=self.session), \
             patch.object(worker, 'get_sk_uid', return_value=('sk', 'uid')), \
             patch.object(worker, 'process_file', side_effect=error) as process, \
             redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, 'lease lost'):
                worker.run(1, 'run1', self.tmp.name, self.db, 'unused', timeout_retries=2)
        self.assertEqual(process.call_count, 1)
        self.assertEqual(queue.counts(self.db), {'pending': 1})

    def test_session_timeout_is_fatal_before_any_claim(self):
        self.session.get.side_effect = requests.ReadTimeout('private URL')
        with patch.object(worker, 'make_session', return_value=self.session), \
             patch.object(worker, 'process_file') as process:
            with self.assertRaises(api.DownloadError) as caught:
                worker.run(1, 'run1', self.tmp.name, self.db, 'unused', timeout_retries=2)
        self.assertTrue(caught.exception.fatal)
        process.assert_not_called()
        self.assertEqual(queue.counts(self.db), {'pending': 1})


if __name__ == '__main__':
    unittest.main()
