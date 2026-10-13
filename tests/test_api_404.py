import unittest
from unittest.mock import Mock

from unlim_api import api_call, DownloadError


class Error404Tests(unittest.TestCase):
    def test_safe_server_code_preserves_http_status(self):
        response = Mock(status_code=404)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.json.return_value = {'error': {'code': 70, 'title': 'private path'}}
        session = Mock()
        session.post.return_value = response
        with self.assertRaises(DownloadError) as caught:
            api_call(session, 'secret', '123', 'mpfs/info', {'path': '/disk/private'})
        self.assertEqual(caught.exception.code, 'mpfs/info: HTTP 404; API 70')
        self.assertFalse(caught.exception.fatal)
        self.assertNotIn('private', str(caught.exception))

    def test_arbitrary_error_text_is_not_logged(self):
        for payload in ({'error': {'code': 'https://private.invalid/?token=secret'}},
                        {'error': 'private reason'}, ['unexpected']):
            with self.subTest(payload=payload):
                response = Mock(status_code=404)
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                response.json.return_value = payload
                session = Mock()
                session.post.return_value = response
                with self.assertRaises(DownloadError) as caught:
                    api_call(session, 's', 'u', 'mpfs/info')
                self.assertEqual(caught.exception.code, 'mpfs/info: HTTP 404')


if __name__ == '__main__':
    unittest.main()
