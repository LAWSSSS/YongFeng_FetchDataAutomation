"""Presentation-only removal. Worker saves cannot resurrect a hidden record."""
import json

from .domain import now_iso
from .videos import Conflict


class RecordVisibility:
    def __init__(self, db, namespace='slicing'):
        assert namespace in {'slicing', 'collection', 'receipts', 'receipt_items'}
        self.db = db
        self.namespace = namespace
        self.hidden_table = f'simple_{namespace}_hidden_records'
        self.actions_table = f'simple_{namespace}_record_actions'
        db.execute(f'CREATE TABLE IF NOT EXISTS {self.hidden_table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.execute(f'CREATE TABLE IF NOT EXISTS {self.actions_table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.commit()

    def hidden(self):
        return {r[0] for r in self.db.execute(f'SELECT id FROM {self.hidden_table}')}

    def _action(self, identity, signature):
        row = self.db.execute(f'SELECT payload FROM {self.actions_table} WHERE id=?', (identity,)).fetchone()
        if row:
            saved = json.loads(row[0])
            if saved['signature'] != signature:
                raise Conflict('请求编号已用于其他记录操作。')
            return saved
        return None

    def prepare_hide(self, store, request_id, selections):
        choices = sorted(selections, key=lambda s: s['id'])
        if len({s['id'] for s in choices}) != len(choices):
            raise ValueError('不能重复选择同一条记录。')
        signature = {'kind': 'hide', 'selections': choices}
        saved = self._action(request_id, signature)
        if saved:
            return saved['result'], None
        hidden = self.hidden()
        for choice in choices:
            row = store.get('items', choice['id']) if self.namespace == 'slicing' else store.get(choice['id'])
            if not row:
                raise ValueError('记录不存在，请刷新列表。')
            if int(row.get('revision', 0)) != choice['expectedRevision']:
                raise Conflict('记录已有新进度，未删除；请刷新后重新选择。')
            if choice['id'] in hidden:
                raise Conflict('记录已从列表删除，请刷新。')
        result = {'removalId': request_id, 'ids': [s['id'] for s in choices]}
        action = {'signature': signature, 'result': result, 'at': now_iso()}
        return result, action

    def write_hide(self, request_id, action):
        if action is None:
            return
        self.db.executemany(f'INSERT INTO {self.hidden_table} VALUES(?,?)',
            [(identity, json.dumps({'removalId': request_id, 'at': action['at']})) for identity in action['result']['ids']])
        self.db.execute(f'INSERT INTO {self.actions_table} VALUES(?,?)', (request_id, json.dumps(action)))

    def hide(self, store, request_id, selections):
        result, action = self.prepare_hide(store, request_id, selections)
        with self.db:
            self.write_hide(request_id, action)
        return result

    def prepare_restore(self, request_id, removal_id):
        signature = {'kind': 'restore', 'removalId': removal_id}
        saved = self._action(request_id, signature)
        if saved:
            return saved['result'], None
        original = self.db.execute(f'SELECT payload FROM {self.actions_table} WHERE id=?', (removal_id,)).fetchone()
        original = json.loads(original[0]) if original else None
        if not original or original['signature']['kind'] != 'hide':
            raise ValueError('删除记录不存在。')
        ids = original['result']['ids']
        for identity in ids:
            row = self.db.execute(f'SELECT payload FROM {self.hidden_table} WHERE id=?', (identity,)).fetchone()
            if row and json.loads(row[0])['removalId'] != removal_id:
                raise Conflict('记录后来有新的删除操作，请刷新后再恢复。')
        result = {'restored': ids}
        return result, {'signature': signature, 'result': result, 'at': now_iso()}

    def write_restore(self, request_id, action):
        if action is None:
            return
        self.db.executemany(f'DELETE FROM {self.hidden_table} WHERE id=?', [(i,) for i in action['result']['restored']])
        self.db.execute(f'INSERT INTO {self.actions_table} VALUES(?,?)', (request_id, json.dumps(action)))

    def restore(self, request_id, removal_id):
        result, action = self.prepare_restore(request_id, removal_id)
        with self.db:
            self.write_restore(request_id, action)
        return result


def hide_groups(request_id, groups):
    """Validate the whole displayed snapshot before writing any visibility changes."""
    prepared = [(visibility, *visibility.prepare_hide(store, request_id, choices))
                for visibility, store, choices in groups]
    with groups[0][0].db:
        for visibility, _, action in prepared:
            visibility.write_hide(request_id, action)
    return {'removalId': request_id, 'ids': [i for _, result, _ in prepared for i in result['ids']]}


def restore_groups(request_id, removal_id, visibilities):
    prepared = [(visibility, *visibility.prepare_restore(request_id, removal_id)) for visibility in visibilities]
    with visibilities[0].db:
        for visibility, _, action in prepared:
            visibility.write_restore(request_id, action)
    return {'restored': [i for _, result, _ in prepared for i in result['restored']]}
