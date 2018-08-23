from sys import stdin

import asyncio
import pickle
from datetime import datetime

loop = asyncio.get_event_loop()


def print_debug(*args, **kwargs):
    print(datetime.now(), *args, **kwargs)


def rpc_decode(s):
    return pickle.loads(s)


def rpc_encode(o):
    return pickle.dumps(o)


class RPC:
    def __init__(self):
        self._cb_table = {}

    async def _server_handle(self, reader, writer):
        try:
            data = await reader.read()

            cmd = rpc_decode(data)
            event, args, kwargs = cmd
            cb = self._cb_table[event]
            ret = cb(*args, **kwargs) if not asyncio.iscoroutine(cb) else await cb(*args, **kwargs)
            s = rpc_encode(ret)

            writer.write(s)
            await writer.drain()
            writer.close()

            print_debug('Receive:', cmd, 'Reply:', ret)

        except ConnectionError:
            pass

    def schedule_server(self, host, port):
        coro = asyncio.start_server(self._server_handle, host, port, loop=loop)
        asyncio.ensure_future(coro)

        print_debug('Start:', host, port)

    def add_event_listener(self, event, cb):
        self._cb_table[event] = cb


class RPCProxy:
    def __init__(self, host, port):
        self._host = host
        self._port = port

    def __getattr__(self, name):
        async def call(host, port, event, *args, **kwargs):
            reader, writer = await asyncio.open_connection(host, port, loop=loop)

            cmd = (event, args, kwargs)
            s = rpc_encode(cmd)
            writer.write(s)
            writer.write_eof()

            data = await reader.read()
            writer.close()
            ret = rpc_decode(data)

            print_debug('Request:', cmd, 'Receive:', ret)

            return ret

        def func(*args, **kwargs):
            return call(self._host, self._port, name, *args, **kwargs)

        return func


# --- RPC ---


class Storage:
    def __init__(self, filename):
        self._filename = filename

    def load(self):
        with open(self._filename, 'rb') as f:
            ret = pickle.load(f)
        return ret

    def dump(self, o):
        with open(self._filename, 'wb') as f:
            pickle.dump(o, f)


# --- Storage ---


class Paxos:
    def __init__(self, host, port, servers, storage, rpc):
        self._host = host
        self._port = port
        self._servers = servers
        self._storage = storage
        self._over_half_num = len(servers) // 2 + 1

        rpc.add_event_listener('prepare', self._on_prepare)
        rpc.add_event_listener('accept', self._on_accept)
        rpc.add_event_listener('learn', self._on_learn)

        # 状态
        try:
            self._seq, self._proposal_seq, self._proposal_val, self._proposal_unanimous = self._storage.load()
        except FileNotFoundError:
            self._seq = (0, self._host, self._port)
            self._proposal_seq = self._seq
            self._proposal_val = None
            self._proposal_unanimous = False

    async def propose(self, val):

        print_debug('Propose:', val)

        if self._proposal_unanimous:
            return self._proposal_val

        local_seq = self._seq
        local_proposal_seq = self._proposal_seq
        local_proposal_val = self._proposal_val
        local_proposal_unanimous = self._proposal_unanimous

        # await 时状态有可能被修改, 若发生, 则放弃
        def is_state_changed_then_handle(futs):
            if local_seq != self._seq \
                    or local_proposal_seq != self._proposal_seq \
                    or local_proposal_val != self._proposal_val \
                    or local_proposal_unanimous != self._proposal_unanimous:
                for f in futs:
                    f.cancel()
                return True
            return False

        # prepare
        cnt = 0
        max_proposal_seq = (0, '', '')
        fs = [asyncio.ensure_future(s.prepare(self._seq))
              for s in self._servers]
        for f in asyncio.as_completed(fs):
            try:
                r = await f
            except ConnectionError:
                continue
            if is_state_changed_then_handle(fs):
                return None

            seq, proposal_seq, proposal_val = r
            if seq != self._seq:
                assert seq > self._seq
                self._seq = (seq[0] + 1, self._host, self._port)
                self._store()
                return None

            if proposal_val is not None and proposal_seq > max_proposal_seq:
                max_proposal_seq = proposal_seq
                val = proposal_val

            cnt += 1
            if cnt >= self._over_half_num:
                break
        else:
            return None

        # accept
        cnt = 0
        fs = [asyncio.ensure_future(s.accept(self._seq, val))
              for s in self._servers]
        for f in asyncio.as_completed(fs):
            try:
                seq = await f
            except ConnectionError:
                continue
            if is_state_changed_then_handle(fs):
                return None

            if seq != self._seq:
                assert seq > self._seq
                self._seq = (seq[0] + 1, self._host, self._port)
                self._store()
                return None

            cnt += 1
            if cnt >= self._over_half_num:
                break
        else:
            return None

        # learn
        for s in self._servers:
            asyncio.ensure_future(s.learn(val))
        return val

    def _store(self):
        self._storage.dump((self._seq,
                            self._proposal_seq, self._proposal_val,
                            self._proposal_unanimous))

    def _on_prepare(self, seq):
        if seq > self._seq:
            self._seq = seq
            self._store()
        return self._seq, self._proposal_seq, self._proposal_val

    def _on_accept(self, seq, val):
        if seq >= self._seq:
            self._seq = seq
            self._proposal_seq = seq
            self._proposal_val = val
            self._store()
        return self._seq

    def _on_learn(self, val):
        self._proposal_val = val
        self._proposal_unanimous = True
        self._store()


# --- Paxos ---

def run_cli(host, port, hosts_ports, filename):
    rpc = RPC()
    servers = [RPCProxy(h, p) for h, p in hosts_ports]
    storage = Storage(filename)
    paxos = Paxos(host, port, servers, storage, rpc)

    def stdin_handle(*_):
        asyncio.ensure_future(paxos.propose(stdin.readline().strip()))

    rpc.schedule_server(host, port)
    loop.add_reader(stdin.fileno(), stdin_handle)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

# --- CLI ---
