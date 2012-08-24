import socket

class MuninClient(object):
    def __init__(self, host, port=4949):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock.recv(4096) # welcome, TODO: receive all

    def _command(self, cmd, term):
        self.sock.send("%s\n" % cmd)
        buf = ""
        while term not in buf:
            try:
                buf += self.sock.recv(4096)
            except Exception, e:
                print "Error:", e
        return buf.split(term)[0]

    def list(self):
        return self._command('list', '\n').split(' ')

    def fetch(self, service):
        data = self._command("fetch %s" % service, ".\n")
        if data.startswith('#'):
            values = False
        else:
            values = {}
        if values != False:
            for line in data.split('\n'):
                    if line:
                        k, v = line.split(' ', 1)
                        values[k.split('.')[0]] = v.rstrip()
        return values

