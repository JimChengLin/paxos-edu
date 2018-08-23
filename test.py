from paxos import run_cli

hosts_ports = (
    ('127.0.0.1', '8881'),
    ('127.0.0.1', '8882'),
    ('127.0.0.1', '8883'),
    ('127.0.0.1', '8884'),
    ('127.0.0.1', '8885'),
)

run_cli(*hosts_ports[0], hosts_ports, hosts_ports[0][1])
