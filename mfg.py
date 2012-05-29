import sys
import socket
import time
import subprocess
import ConfigParser

def facterconfig(file):
    c=ConfigParser.ConfigParser()
    if c.read(file):
        if c.has_section('facter'):
                append=c.get('facter','append')
        else:
                append=False
    else:
        append=False
    return append

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

def facter2dict( lines ):
        res = {}
        for line in lines:
                k, v = line.split(' => ')
                res[k] = v.rstrip()
        return res

def facter():
        p = subprocess.Popen( ['facter','-p'], stdout=subprocess.PIPE )
        p.wait()
        lines = p.stdout.readlines()
        return facter2dict( lines )

def facterstringfromconfig(file):
    config=facterconfig(file)
    if config != False:
        to_append=config.split('.')
        f=facter()
        append=''
        for fact in to_append:
            append+=f[fact]+'.'
    else:
        append=socket.gethostname()+'.'
    return append

def send_to_carbon(message,file='./config.ini'):
    c=ConfigParser.ConfigParser()
    if c.read(file):
        if c.has_section('carbon'):
                host=c.get('carbon','host')
                port=c.get('carbon','port')
                sock = socket.socket()
                sock.connect((host, int(port)))
                sock.sendall(message)
                sock.close()
        else:
                return False
    else:
        return False

while True:
        m=MuninClient('127.0.0.1')
        append=facterstringfromconfig('./config.ini')
        list=m.list()
        for item in list:
                values=m.fetch(item)
                for key in values:
                        message = 'servers'+'.'+append+item+'.'+key+' '+values[key]+' %d\n' % int(time.time())
                        send_to_carbon(message)

        time.sleep( 60 )
