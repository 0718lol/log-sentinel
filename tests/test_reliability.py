import asyncio
import os
from pathlib import Path
import tempfile
import unittest
import httpx
from agent import LogAgent
from spool import Spool


class SpoolTests(unittest.TestCase):
    def test_atomic_checkpoint_backpressure_and_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.db'
            spool = Spool(path, 1)
            self.assertTrue(spool.append({'message': 'a'}, ('log', 'inode', 2, b'a\n')))
            event = spool.batch(1)[0]
            self.assertFalse(spool.append({'message': 'b'}, ('log', 'inode', 4, b'b\n')))
            self.assertEqual(spool.checkpoint('log')[1], 2)
            with self.assertRaises(RuntimeError):
                Spool(path)
            spool.close()
            spool = Spool(path)
            self.assertEqual(spool.batch(1)[0], event)
            spool.acknowledge([event['event_id']])
            self.assertEqual(spool.pending_count(), 0)
            spool.close()


class AgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_rotation_truncate_and_partial_utf8(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'app.log'
            state = str(Path(directory) / 'state.db')
            path.write_bytes('第一行\npartial'.encode())
            async def collect(expected):
                agent = LogAgent(str(path), 'http://test', 'test', 10, .01, state_path=state)
                agent.running = True
                task = asyncio.create_task(agent.tail_file())
                try:
                    for _ in range(100):
                        if agent.spool.pending_count() == expected:
                            return agent.spool.batch(100)
                        if task.done():
                            task.result()
                        await asyncio.sleep(.02)
                    self.fail('tailer did not collect expected events')
                finally:
                    agent.running = False
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    agent.spool.close()
                    await agent.client.aclose()
            first = await collect(1)
            with path.open('ab') as stream:
                stream.write(b' complete\n')
            second = await collect(2)
            self.assertEqual(second[0], first[0])
            self.assertEqual(second[1]['message'], 'partial complete')
            path.rename(path.with_suffix('.log.1'))
            path.write_text('rotated\n')
            third = await collect(3)
            self.assertEqual(third[-1]['message'], 'rotated')
            path.write_text('replacement longer than previous\n')
            fourth = await collect(4)
            self.assertEqual(fourth[-1]['message'], 'replacement longer than previous')

    async def test_ack_loss_retry_and_permanent_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            agent = LogAgent('test.log', 'http://test', 'test', 10, .01,
                             state_path=str(Path(directory) / 'state.db'))
            agent.running = True
            agent.spool.append({'timestamp': '2026-01-01T00:00:00Z', 'message': 'a', 'level': 'INFO', 'source': 'test'})
            batch = agent.spool.batch(10)
            await agent.client.aclose()
            async def reject(request):
                return httpx.Response(401)
            agent.client = httpx.AsyncClient(transport=httpx.MockTransport(reject))
            with self.assertRaises(RuntimeError):
                await agent.send_batch_with_retry(batch)
            self.assertEqual(agent.spool.batch(10), batch)
            await agent.client.aclose()
            agent.client = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={'acknowledged_ids': [batch[0]['event_id']]})))
            await agent.send_batch_with_retry(batch)
            self.assertEqual(agent.spool.pending_count(), 0)
            await agent.client.aclose()
            agent.spool.close()


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_has_no_second_window_entry_and_legacy_works(self):
        import database
        with tempfile.TemporaryDirectory() as directory:
            old = database.DB_PATH
            database.DB_PATH = str(Path(directory) / 'server.db')
            try:
                import server
                database.init_db()
                server.sliding_window.clear()
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url='http://test') as client:
                    entry = {'timestamp': '2026-01-01T00:00:00Z', 'level': 'INFO', 'message': 'a', 'source': 'test', 'event_id': 'stable'}
                    first = await client.post('/api/logs/ingest', json=[entry])
                    second = await client.post('/api/logs/ingest', json=[entry])
                    self.assertEqual(first.json()['count'], 1)
                    self.assertEqual(second.json()['duplicates'], 1)
                    self.assertEqual(second.json()['acknowledged_ids'], ['stable'])
                    self.assertEqual(len(database.get_recent_logs()), 1)
                    entry.pop('event_id')
                    self.assertEqual((await client.post('/api/logs/ingest', json=[entry])).json()['count'], 1)
            finally:
                database.DB_PATH = old

class DatabaseTests(unittest.TestCase):
    def test_migration_and_concurrent_duplicate_ingestion(self):
        import concurrent.futures
        import sqlite3
        import database
        with tempfile.TemporaryDirectory() as directory:
            previous = database.DB_PATH
            database.DB_PATH = str(Path(directory) / 'old.db')
            try:
                with sqlite3.connect(database.DB_PATH) as conn:
                    conn.execute('CREATE TABLE logs(id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL, source TEXT NOT NULL)')
                    conn.execute("INSERT INTO logs VALUES(1,'old','INFO','legacy','test')")
                database.init_db()
                entry = {'timestamp': 'now', 'level': 'INFO', 'message': 'new', 'source': 'test', 'event_id': 'same'}
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    results = list(executor.map(lambda _: database.save_logs([entry]), range(8)))
                self.assertEqual(sum(len(result) for result in results), 1)
                self.assertEqual(len(database.get_recent_logs()), 2)
                database.init_db()
            finally:
                database.DB_PATH = previous

    def test_checkpoint_rolls_back_on_failed_outbox_write(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = Spool(Path(directory) / 'state.db')
            spool.append({'message': 'first'}, ('log', 'inode', 6, b'first\n'))
            spool.db.execute("CREATE TRIGGER reject_event BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'disk write simulation'); END")
            import sqlite3
            with self.assertRaises(sqlite3.IntegrityError):
                spool.append({'message': 'second'}, ('log', 'inode', 13, b'second\n'))
            self.assertEqual(spool.checkpoint('log')[1], 6)
            self.assertEqual(spool.pending_count(), 1)
            spool.close()
