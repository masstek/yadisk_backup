import hashlib
from contextlib import closing
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import requests
import unlim_api as api
import unlim_state as queue
import unlim_worker as worker
import unlim_download as collect


class Response:
    def __init__(self, content=b'data', payload=None, status=200, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.payload = payload
        self.content = content
    def json(self): return self.payload
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def close(self): pass
    def iter_content(self, _):
        if isinstance(self.content, Exception): raise self.content
        yield self.content


class ApiTests(unittest.TestCase):
    def test_single_original_contract_and_timeout(self):
        s = Mock()
        s.post.return_value = Response(payload={'meta': {'size': 4, 'md5': '0'*32,
            'sizes': [{'name': 'XXXL', 'url': 'https://preview.invalid/a'},
                      {'name': 'ORIGINAL', 'url': '//download.invalid/a'}]}})
        info = api.original_info(s, 'secret', '123', 'photounlim:/a.jpg', api.Timeouts())
        self.assertEqual(info['url'], 'https://download.invalid/a')
        call = s.post.call_args.kwargs
        self.assertEqual(call['json']['apiMethod'], 'mpfs/info')
        self.assertEqual(call['json']['requestParams'], {'path': '/photounlim/a.jpg'})
        self.assertEqual(call['timeout'], (5, 15))
        self.assertFalse(call['allow_redirects'])

    def test_resource_path_preserves_newline_in_filename(self):
        for prefix in ('photounlim', 'disk'):
            for uid in ('', '123:'):
                self.assertEqual(api.resource_path(f'{uid}{prefix}:/folder/a\nb.jpg'),
                                 f'/{prefix}/folder/a\nb.jpg')

    def test_missing_original_never_falls_back_to_preview(self):
        s = Mock()
        s.post.return_value = Response(payload={'meta': {'sizes': [{'name': 'XXXL', 'url': 'https://preview.invalid/a'}]}})
        with self.assertRaisesRegex(api.DownloadError, 'ORIGINAL'):
            api.original_info(s, 's', 'u', '/photounlim/a', api.Timeouts())
        self.assertEqual(s.post.call_count, 1)

    def test_http200_error_reported_without_secrets(self):
        with self.assertRaisesRegex(api.DownloadError, 'CKEY') as ctx:
            api.checked_json(Response(payload={'error': {'code': 'CKEY', 'ckey': 'TOPSECRET'}}), 'info')
        self.assertTrue(ctx.exception.fatal)
        self.assertNotIn('TOPSECRET', str(ctx.exception))

    def test_timeout_does_not_retry_or_log_url(self):
        s = Mock()
        s.post.side_effect = requests.Timeout('https://secret.invalid/?sk=SECRET')
        with self.assertRaises(api.DownloadError) as ctx:
            api.api_call(s, 's', 'u', 'mpfs/info')
        self.assertEqual(s.post.call_count, 1)
        self.assertTrue(ctx.exception.is_timeout)
        self.assertNotIn('SECRET', str(ctx.exception))

    def test_auth_redirect_stops_run(self):
        with self.assertRaises(api.DownloadError) as ctx:
            api.check_http(Response(status=302), 'info', auth=True)
        self.assertTrue(ctx.exception.fatal)

    def test_rate_limit_preserves_retry_after(self):
        with self.assertRaises(api.DownloadError) as ctx:
            api.check_http(Response(status=429, headers={'Retry-After': '120'}), 'info')
        self.assertEqual(ctx.exception.retry_after, 120)
        self.assertTrue(ctx.exception.fatal)

    def test_resource_403_is_skipped_but_ckey_stops_run(self):
        s = Mock()
        s.post.return_value = Response(status=403, payload={'error': {'code': 'RESOURCE_UNAVAILABLE'}})
        with self.assertRaises(api.DownloadError) as ctx:
            api.api_call(s, 's', 'u', 'mpfs/info')
        self.assertFalse(ctx.exception.fatal)
        s.post.return_value = Response(status=403, payload={'error': {'code': 'CKEY'}})
        with self.assertRaises(api.DownloadError) as ctx:
            api.api_call(s, 's', 'u', 'mpfs/info')
        self.assertTrue(ctx.exception.fatal)

    def test_explicit_timeouts_can_be_configured(self):
        with patch.dict(os.environ, {'DOWNLOAD_CONNECT_TIMEOUT': '2', 'DOWNLOAD_READ_TIMEOUT': '7'}):
            self.assertEqual(api.Timeouts.from_env().pair, (2, 7))


class DatabaseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = os.path.join(self.tmp.name, 'state.db')
        queue.ensure_schema(self.db)
    def add(self, fid='photounlim:/a.jpg', **fields):
        with queue.transaction(self.db) as c:
            c.execute('INSERT INTO unlim_files(file_id,name,size) VALUES (?,?,?)', (fid, 'same.jpg', 4))
            for key, value in fields.items():
                c.execute(f'UPDATE unlim_files SET {key}=? WHERE file_id=?', (value, fid))


class QueueTests(DatabaseTest):
    def test_pending_photounlim_precedes_disk_and_failed(self):
        self.add('disk:/a.jpg', download_path='/disk/a.jpg')
        self.add('photounlim:/z.jpg', download_path='/photounlim/z.jpg')
        self.add('disk:/old.jpg', download_path='/disk/old.jpg', status='failed')
        first = queue.claim(self.db, 1, 'run1')
        second = queue.claim(self.db, 1, 'run1')
        third = queue.claim(self.db, 1, 'run1')
        self.assertEqual([first['file_id'], second['file_id'], third['file_id']],
                         ['photounlim:/z.jpg', 'disk:/a.jpg', 'disk:/old.jpg'])

    def test_pending_photounlim_id_is_fallback_without_download_path(self):
        self.add('disk:/a.jpg', download_path='/disk/a.jpg')
        self.add('photounlim:/z.jpg')
        self.assertEqual(queue.claim(self.db, 1, 'run1')['file_id'], 'photounlim:/z.jpg')

    def test_failure_once_per_run_and_retry_next_run(self):
        self.add()
        a = queue.claim(self.db, 1, 'run1')
        queue.fail(self.db, a, 'file: Timeout')
        self.assertIsNone(queue.claim(self.db, 2, 'run1'))
        b = queue.claim(self.db, 1, 'run2')
        self.assertEqual(b['file_id'], a['file_id'])
        self.assertEqual(b['attempts'], 2)
    def test_unavailable_file_does_not_block_other_files(self):
        self.add('photounlim:/a.jpg')
        self.add('photounlim:/b.jpg')
        a = queue.claim(self.db, 1, 'run1')
        queue.fail(self.db, a, 'file: HTTP 404')
        b = queue.claim(self.db, 1, 'run1')
        self.assertNotEqual(a['file_id'], b['file_id'])
    def test_missing_downloaded_copy_is_reopened(self):
        self.add(local_path=os.path.join(self.tmp.name,'lost.jpg'), status='downloaded')
        self.assertEqual(queue.reconcile_missing(self.db), 1)
        self.assertEqual(queue.counts(self.db), {'pending':1})

    def test_retry_after_is_honored_across_runs(self):
        self.add()
        a = queue.claim(self.db, 1, 'r1')
        queue.fail(self.db, a, 'temporarily unavailable', delay=120)
        self.assertIsNone(queue.claim(self.db, 1, 'r2'))

    def test_atomic_claim_between_workers(self):
        self.add()
        claimed = []
        def claim(): claimed.append(queue.claim(self.db, 1, 'same-run'))
        threads = [threading.Thread(target=claim) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(sum(c is not None for c in claimed), 1)
    def test_expired_worker_cannot_publish(self):
        self.add()
        a = queue.claim(self.db, 1, 'run1')
        with queue.transaction(self.db) as c:
            c.execute('UPDATE unlim_files SET lease_until=0')
        b = queue.claim(self.db, 2, 'run2')
        path = Path(self.tmp.name, 'keep.jpg')
        tmp = Path(self.tmp.name, 'new.part')
        path.write_bytes(b'old')
        tmp.write_bytes(b'new')
        with self.assertRaises(RuntimeError):
            queue.complete(self.db, a, str(path), 'sha', str(tmp))
        self.assertEqual(path.read_bytes(), b'old')
        self.assertTrue(queue.renew(self.db, b))
    def test_expired_worker_cannot_update_original_metadata(self):
        self.add(md5='old')
        a = queue.claim(self.db, 1, 'run1')
        with queue.transaction(self.db) as c:
            c.execute('UPDATE unlim_files SET lease_until=0')
        with self.assertRaisesRegex(RuntimeError, 'lease lost'):
            queue.update_original_metadata(self.db, a, 99, 'stale')
        with queue.connection(self.db) as c:
            row = c.execute('SELECT size,md5 FROM unlim_files').fetchone()
        self.assertEqual((row['size'], row['md5']), (4, 'old'))
    def test_legacy_migration_requeues_unverified_but_keeps_files(self):
        legacy = os.path.join(self.tmp.name, 'legacy.db')
        with closing(sqlite3.connect(legacy)) as c:
            c.executescript(queue.SCHEMA)
            c.execute("INSERT INTO unlim_files(file_id,status) VALUES ('a','downloaded')")
            c.commit()
        queue.ensure_schema(legacy)
        self.assertEqual(queue.counts(legacy), {'pending': 1})
        queue.ensure_schema(legacy)
        self.assertEqual(queue.counts(legacy), {'pending': 1})
    def test_same_name_files_have_different_destinations(self):
        a = worker.file_destination(self.tmp.name, {'file_id': 'photounlim:/a', 'name': 'same.jpg'})
        b = worker.file_destination(self.tmp.name, {'file_id': 'photounlim:/b', 'name': 'same.jpg'})
        self.assertNotEqual(a, b)
    def test_rate_limit_is_shared_without_waiting_in_worker(self):
        self.add()
        a = queue.claim(self.db, 1, 'r1')
        queue.fail(self.db, a, '429', delay=120, rate_limited=True)
        self.assertIsNone(queue.claim(self.db, 2, 'r2'))
        self.assertGreater(queue.cooldown_remaining(self.db), 100)


class DownloadTests(DatabaseTest):
    def setUp(self):
        super().setUp()
        self.add()
        self.job = queue.claim(self.db, 1, 'r1')
        self.info = {'url': 'https://download.invalid/a', 'size': 4, 'md5': hashlib.md5(b'data').hexdigest()}
    def test_download_verifies_and_records_one_file(self):
        s = Mock()
        s.get.return_value = Response(headers={'Content-Length': '4'})
        with patch.object(worker, 'original_info', return_value=self.info):
            result = worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        self.assertEqual(result, 4)
        self.assertEqual(queue.counts(self.db), {'downloaded': 1})
        with queue.connection(self.db) as c:
            row = c.execute('SELECT local_path,sha256,size FROM unlim_files').fetchone()
        self.assertEqual(Path(row['local_path']).read_bytes(), b'data')
        self.assertEqual(row['sha256'], hashlib.sha256(b'data').hexdigest())
        self.assertEqual(s.get.call_args.kwargs['timeout'], (5, 15))
    def test_timeout_keeps_existing_file_and_removes_part(self):
        dst = Path(worker.file_destination(self.tmp.name, self.job))
        dst.parent.mkdir(parents=True)
        dst.write_bytes(b'KEEP')
        s = Mock()
        s.get.return_value = Response(content=requests.ReadTimeout('signed-secret'))
        with patch.object(worker, 'original_info', return_value=self.info):
            with self.assertRaisesRegex(api.DownloadError, 'ReadTimeout'):
                worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        self.assertEqual(dst.read_bytes(), b'KEEP')
        self.assertEqual(list(dst.parent.glob('*.part')), [])
        self.assertNotIn('downloaded', queue.counts(self.db))
    def test_bad_hash_does_not_publish(self):
        s = Mock()
        s.get.return_value = Response(content=b'evil')
        with patch.object(worker, 'original_info', return_value=self.info):
            with self.assertRaisesRegex(api.DownloadError, 'MD5'):
                worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        self.assertFalse(Path(worker.file_destination(self.tmp.name, self.job)).exists())
    def test_original_metadata_survives_failed_transfer(self):
        fresh = {**self.info, 'size': 7, 'md5': 'a' * 32}
        s = Mock()
        s.get.return_value = Response(status=503)
        with patch.object(worker, 'original_info', return_value=fresh):
            with self.assertRaises(api.DownloadError):
                worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        queue.fail(self.db, self.job, 'file: HTTP 503')
        with queue.connection(self.db) as c:
            row = c.execute('SELECT size,md5,status FROM unlim_files').fetchone()
        self.assertEqual((row['size'], row['md5'], row['status']), (7, 'a' * 32, 'failed'))

    def test_unknown_original_size_stays_null_after_failed_transfer(self):
        with queue.transaction(self.db) as c:
            c.execute('UPDATE unlim_files SET size=NULL')
        s = Mock()
        s.get.return_value = Response(status=503)
        with patch.object(worker, 'original_info', return_value={**self.info, 'size': None}):
            with self.assertRaises(api.DownloadError):
                worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        with queue.connection(self.db) as c:
            row = c.execute('SELECT size,md5 FROM unlim_files').fetchone()
        self.assertIsNone(row['size'])
        self.assertEqual(row['md5'], self.info['md5'])

    def test_lost_lease_after_info_stops_before_download(self):
        def expire_lease(*_):
            with queue.transaction(self.db) as c:
                c.execute('UPDATE unlim_files SET lease_until=0')
            return self.info

        s = Mock()
        with patch.object(worker, 'original_info', side_effect=expire_lease):
            with self.assertRaisesRegex(RuntimeError, 'lease lost'):
                worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        s.get.assert_not_called()
    def test_fresh_missing_hash_does_not_trust_old_snapshot_hash(self):
        self.job['md5'] = hashlib.md5(b'KEEP').hexdigest()
        dst = Path(worker.file_destination(self.tmp.name, self.job))
        dst.parent.mkdir(parents=True)
        dst.write_bytes(b'KEEP')
        s = Mock()
        s.get.return_value = Response(content=b'NEW!', headers={'Content-Length':'4'})
        with patch.object(worker, 'original_info', return_value={**self.info, 'md5': None}):
            worker.process_file(s, 's', 'u', self.job, self.tmp.name, self.db, api.Timeouts())
        self.assertEqual(dst.read_bytes(), b'NEW!')
        self.assertEqual(s.get.call_count, 1)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = os.path.join(self.tmp.name, 'state.db')
        self.patches = [patch.object(collect, 'DB_FILE', self.db), patch.object(collect, '_tls', threading.local())]
        for p in self.patches: p.start(); self.addCleanup(p.stop)
        collect.db_init()
        self.addCleanup(lambda: collect.db().close())
    def test_resources_metadata_and_missing_reference_are_retained(self):
        response = {'resources': {'fetched': [{'id':'hash1','path':'/photounlim/a.jpg','name':'a.jpg','meta':{'size':4,'md5':'a'*32}}], 'missing':['photounlim:/b.jpg']},
                    'clusters':{'fetched':[{'id':'day','items':['hash1','photounlim:/b.jpg']}]}}
        records = collect._resource_records(response, 'day', 0, 200)
        self.assertEqual(collect._save_resources(records, 'day'), 2)
        rows = list(collect.db().execute('SELECT * FROM unlim_files ORDER BY file_id'))
        self.assertEqual(rows[0]['download_path'], '/photounlim/a.jpg')
        self.assertEqual(rows[0]['size'], 4)
        self.assertEqual(rows[0]['md5'], 'a'*32)
        self.assertIsNone(rows[1]['size'])
    def test_repeat_collect_detects_new_photo_in_same_cluster(self):
        collect.save_clusters([{'id':'day','size':1}])
        collect.db().execute("UPDATE unlim_clusters SET fetched=1")
        collect.save_clusters([{'id':'day','size':2}])
        row = collect.db().execute('SELECT * FROM unlim_clusters').fetchone()
        self.assertEqual((row['size'],row['fetched']), (2,0))
    def test_partial_cluster_is_not_marked_complete(self):
        collect.save_clusters([{'id':'day','size':2}])
        responses = [{'resources':{'fetched':[{'path':'/photounlim/a'}]}}, {'resources':{'fetched':[]}}]
        with patch.object(collect, 'api_call', side_effect=responses):
            with self.assertRaises(api.DownloadError):
                collect.fetch_cluster_resources(None,'s','u','p',[{'cluster_id':'day','size':2}])
        self.assertEqual(collect.db().execute('SELECT fetched FROM unlim_clusters').fetchone()[0], 0)

    def test_short_cluster_page_ends_without_another_request(self):
        pages = [{'items': [{'id': 'generic', 'cluster_id': 'day', 'size': 1}]}]
        with patch.object(collect, 'api_call', side_effect=pages) as call, redirect_stdout(io.StringIO()) as output:
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertTrue(result.complete)
        self.assertEqual([c['id'] for c in result.items], ['day'])
        self.assertEqual(call.call_count, 1)
        self.assertIn('получено=1/1000, новых=1, всего=1', output.getvalue())

    def test_full_cluster_page_then_short_or_empty_page(self):
        full = [{'cluster_id': f'day-{i}'} for i in range(1000)]
        for tail in ([{'cluster_id': 'last'}], []):
            with self.subTest(tail=bool(tail)):
                pages = [{'items': full}, {'items': tail}]
                with patch.object(collect, 'api_call', side_effect=pages) as call, redirect_stdout(io.StringIO()):
                    result = collect.get_all_clusters(None, 's', 'u', 'p')
                self.assertTrue(result.complete)
                self.assertEqual(len(result.items), 1000 + len(tail))
                self.assertEqual([c.args[4]['offset'] for c in call.call_args_list], [0, 1000])

    def test_overfull_inline_index_is_complete_when_snapshot_ids_agree(self):
        full = [{'cluster_id': f'day-{i}', 'items': [{'path': f'/photounlim/{i}'}]}
                for i in range(1001)]
        snapshot = {'items': [{'id': c['cluster_id'], 'size': 1} for c in reversed(full)]}
        with patch.object(collect, 'api_call', side_effect=[{'items': full}, snapshot]) as call, \
             redirect_stdout(io.StringIO()) as output:
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertTrue(result.complete)
        self.assertEqual(len(result.items), 1001)
        self.assertEqual(result.items[0]['size'], 1)
        self.assertEqual([c.args[3] for c in call.call_args_list],
                         ['intapi/photo-get-clusters', 'intapi/photo-get-snapshot'])
        self.assertIn('сверено кластеров=1001, файлов=1001', output.getvalue())

    def test_overfull_inline_index_is_partial_when_snapshot_disagrees(self):
        full = [{'cluster_id': f'day-{i}', 'items': []} for i in range(1001)]
        snapshot = {'items': [{'id': c['cluster_id'], 'size': 0} for c in full[:-1]]}
        with patch.object(collect, 'api_call', side_effect=[{'items': full}, snapshot]) as call, \
             redirect_stdout(io.StringIO()):
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertFalse(result.complete)
        self.assertEqual(len(result.items), 1001)
        self.assertEqual(call.call_count, 2)
        self.assertIn('не совпал', result.warning)

    def test_overfull_inline_index_is_partial_when_snapshot_size_disagrees(self):
        full = [{'cluster_id': f'day-{i}', 'items': [{'path': f'/photounlim/{i}'}]}
                for i in range(1001)]
        snapshot = {'items': [{'id': c['cluster_id'], 'size': 1} for c in full]}
        snapshot['items'][0]['size'] = 2
        with patch.object(collect, 'api_call', side_effect=[{'items': full}, snapshot]), \
             redirect_stdout(io.StringIO()):
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertFalse(result.complete)
        self.assertEqual(len(result.items), 1001)
        self.assertIn('размеры не совпали', result.warning)

    def test_repeated_full_cluster_page_is_partial_and_keeps_unique_items(self):
        full = [{'cluster_id': f'day-{i}'} for i in range(1000)]
        with patch.object(collect, 'api_call', side_effect=[{'items': full}, {'items': full}]) as call, \
             redirect_stdout(io.StringIO()):
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertFalse(result.complete)
        self.assertIn('повтор полной страницы', result.warning)
        self.assertEqual(len(result.items), 1000)
        self.assertEqual(call.call_count, 2)

    def test_later_request_error_is_partial_and_keeps_unique_items(self):
        full = [{'cluster_id': f'day-{i}'} for i in range(1000)]
        pages = [{'items': full}, api.DownloadError('timeout')]
        with patch.object(collect, 'api_call', side_effect=pages), redirect_stdout(io.StringIO()):
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertFalse(result.complete)
        self.assertEqual(len(result.items), 1000)
        self.assertIn('ошибка запроса', result.warning)

    def test_bad_cluster_item_is_partial_without_attribute_error(self):
        with patch.object(collect, 'api_call', return_value={'items': [{'cluster_id': 'day'}, None]}), \
             redirect_stdout(io.StringIO()):
            result = collect.get_all_clusters(None, 's', 'u', 'p')
        self.assertFalse(result.complete)
        self.assertEqual([c['id'] for c in result.items], ['day'])

    def test_collect_processes_partial_cluster_scan_and_exits_one(self):
        scan = collect.ClusterScanResult([{'id': 'day', 'size': 1}], False,
                                         'photo-get-clusters: повтор полной страницы при offset=1000')
        session = Mock()
        with patch.object(collect, 'make_session', return_value=session), \
             patch.object(collect, 'get_sk_uid', return_value=('s', 'u')), \
             patch.object(collect, 'init_snapshot', return_value='p'), \
             patch.object(collect, 'get_all_clusters', return_value=scan), \
             patch.object(collect, 'fetch_cluster_resources', return_value=1) as fetch, \
             patch.object(collect, 'tqdm', return_value=Mock()), \
             redirect_stdout(io.StringIO()):
            code = collect.cmd_collect()
        self.assertEqual(code, 1)
        self.assertEqual(fetch.call_args.args[4], [{'cluster_id': 'day', 'size': 1}])
        self.assertEqual(collect.db().execute('SELECT cluster_id FROM unlim_clusters').fetchone()[0], 'day')

    def test_collect_fatal_auth_exits_two(self):
        session = Mock()
        with patch.object(collect, 'make_session', return_value=session), \
             patch.object(collect, 'get_sk_uid', side_effect=api.DownloadError('auth', fatal=True)), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(collect.cmd_collect(), 2)

    def test_collect_complete_scan_exits_zero(self):
        session = Mock()
        with patch.object(collect, 'make_session', return_value=session), \
             patch.object(collect, 'get_sk_uid', return_value=('s', 'u')), \
             patch.object(collect, 'init_snapshot', return_value='p'), \
             patch.object(collect, 'get_all_clusters', return_value=collect.ClusterScanResult([], True)), \
             patch.object(collect, 'tqdm', return_value=Mock()), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(collect.cmd_collect(), 0)

    def test_collect_inline_paths_without_metadata_requests(self):
        scan = collect.ClusterScanResult([{'id': 'day', 'size': 2, 'items': [
            {'path': '/photounlim/a.jpg', 'item_id': 'a'},
            {'path': '/photounlim/b.jpg', 'item_id': 'b'}]}], True)
        session = Mock()
        with patch.object(collect, 'make_session', return_value=session), \
             patch.object(collect, 'get_sk_uid', return_value=('s', 'u')), \
             patch.object(collect, 'init_snapshot', return_value='p'), \
             patch.object(collect, 'get_all_clusters', return_value=scan), \
             patch.object(collect, 'fetch_cluster_resources') as fetch, \
             patch.object(collect, 'tqdm', return_value=Mock()), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(collect.cmd_collect(), 0)
        fetch.assert_not_called()
        rows = list(collect.db().execute('SELECT file_id,size,status FROM unlim_files ORDER BY file_id'))
        self.assertEqual([(r['file_id'], r['size'], r['status']) for r in rows], [
            ('photounlim:/a.jpg', None, 'pending'), ('photounlim:/b.jpg', None, 'pending')])
        self.assertEqual(collect.db().execute('SELECT size,fetched FROM unlim_clusters').fetchone()[:], (2, 1))


class RunTests(DatabaseTest):
    def test_repeated_skips_are_aggregated_on_normal_exit(self):
        for letter in 'abc':
            self.add(f'photounlim:/{letter}.jpg')
        s = Mock()
        s.__enter__ = Mock(return_value=s)
        s.__exit__ = Mock(return_value=False)
        out = io.StringIO()
        with patch.object(worker, 'make_session', return_value=s), \
             patch.object(worker, 'get_sk_uid', return_value=('s','u')), \
             patch.object(worker, 'process_file', side_effect=api.DownloadError('mpfs/info: HTTP 404')), \
             patch.object(worker.log, 'warning') as warnings, redirect_stdout(out):
            self.assertEqual(worker.run(1, 'run1', self.tmp.name, self.db, 'fake-cookies'), 1)
        self.assertEqual(warnings.call_count, 3)
        self.assertEqual(queue.counts(self.db), {'failed': 3})
        with queue.connection(self.db) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM unlim_files WHERE error='mpfs/info: HTTP 404'").fetchone()[0], 3)
        self.assertEqual(out.getvalue().count('попытка 1/1 начата'), 3)
        self.assertEqual(out.getvalue().count('пропуск file_key='), 1)
        self.assertIn('пропуски: 2 (mpfs/info: HTTP 404 ×2)', out.getvalue())

    def test_fatal_skip_is_immediate_after_flushing_aggregate(self):
        for letter in 'abcd':
            self.add(f'photounlim:/{letter}.jpg')
        s = Mock()
        s.__enter__ = Mock(return_value=s)
        s.__exit__ = Mock(return_value=False)
        def fail_on_c(*args):
            job = args[3]
            if job['file_id'].endswith('/c.jpg'):
                raise api.DownloadError('CKEY', fatal=True)
            raise api.DownloadError('mpfs/info: HTTP 404')
        out = io.StringIO()
        with patch.object(worker, 'make_session', return_value=s), \
             patch.object(worker, 'get_sk_uid', return_value=('s','u')), \
             patch.object(worker, 'process_file', side_effect=fail_on_c) as process, \
             patch.object(worker.log, 'warning') as warnings, redirect_stdout(out):
            self.assertEqual(worker.run(1, 'run1', self.tmp.name, self.db, 'fake-cookies'), 2)
        self.assertEqual(process.call_count, 3)
        self.assertEqual(warnings.call_count, 3)
        self.assertEqual(queue.counts(self.db), {'failed': 3, 'pending': 1})
        text = out.getvalue()
        self.assertIn('пропуски: 1 (mpfs/info: HTTP 404 ×1)', text)
        self.assertIn('остановка file_key=', text)
        self.assertLess(text.index('пропуски: 1'), text.index('остановка file_key='))

    def test_unavailable_file_is_saved_then_retried_only_on_next_run(self):
        self.add('photounlim:/a.jpg')
        self.add('photounlim:/b.jpg')
        attempts = []
        s = Mock()
        s.__enter__ = Mock(return_value=s)
        s.__exit__ = Mock(return_value=False)
        def process(session, sk, uid, job, root, db_path, timeouts):
            attempts.append(job['file_id'])
            if job['file_id'].endswith('a.jpg') and attempts.count(job['file_id']) == 1:
                raise api.DownloadError('file: ReadTimeout')
            dst = Path(root, job['file_id'].rsplit('/',1)[-1])
            dst.write_bytes(b'data')
            queue.complete(db_path, job, str(dst), hashlib.sha256(b'data').hexdigest())
            return 4
        with patch.object(worker, 'make_session', return_value=s), \
             patch.object(worker, 'get_sk_uid', return_value=('s','u')), \
             patch.object(worker, 'process_file', side_effect=process):
            self.assertEqual(worker.run(1,'run1',self.tmp.name,self.db,'fake-cookies'), 1)
            self.assertEqual(queue.counts(self.db), {'downloaded':1,'failed':1})
            # Another worker in this SAME UI run must not retry the unavailable file.
            worker.run(2,'run1',self.tmp.name,self.db,'fake-cookies')
            self.assertEqual(len(attempts), 2)
            self.assertEqual(worker.run(1,'run2',self.tmp.name,self.db,'fake-cookies'), 0)
        self.assertEqual(attempts, ['photounlim:/a.jpg','photounlim:/b.jpg','photounlim:/a.jpg'])
        self.assertEqual(queue.counts(self.db), {'downloaded':2})

    def test_auth_failure_stops_before_next_file(self):
        self.add('photounlim:/a.jpg')
        self.add('photounlim:/b.jpg')
        s = Mock()
        s.__enter__ = Mock(return_value=s)
        s.__exit__ = Mock(return_value=False)
        with patch.object(worker, 'make_session', return_value=s), \
             patch.object(worker, 'get_sk_uid', return_value=('s','u')), \
             patch.object(worker, 'process_file', side_effect=api.DownloadError('CKEY', fatal=True)) as process:
            self.assertEqual(worker.run(1,'r1',self.tmp.name,self.db,'fake-cookies'), 2)
        self.assertEqual(process.call_count, 1)
        self.assertEqual(queue.counts(self.db), {'failed':1,'pending':1})


if __name__ == '__main__': unittest.main()
