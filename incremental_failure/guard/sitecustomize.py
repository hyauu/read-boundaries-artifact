"""Workers may contact local database subprocesses, never remote providers."""
import socket,os,json,time,ipaddress
_connect=socket.socket.connect;_connect_ex=socket.socket.connect_ex
def check(address):
 if isinstance(address,tuple):
  host=str(address[0]);allowed=host.lower()=='localhost'
  try:allowed=allowed or ipaddress.ip_address(host).is_loopback
  except ValueError:pass
  if not allowed:
   path=os.environ.get('TM_GUARD_LOG')
   if path:
    with open(path,'a',encoding='utf-8') as f:f.write(json.dumps({'event':'REMOTE_NETWORK_DENIED','host':host,'port':address[1],'pid':os.getpid(),'time_ns':time.time_ns()})+'\n')
   raise RuntimeError('This local experiment forbids remote network calls: '+host)
def connect(self,address):check(address);return _connect(self,address)
def connect_ex(self,address):check(address);return _connect_ex(self,address)
socket.socket.connect=connect;socket.socket.connect_ex=connect_ex
