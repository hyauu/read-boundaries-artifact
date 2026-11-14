"""Record and close only descendants observed while this round's worker is alive."""
import ctypes,time
from ctypes import wintypes
from .process_identity import identity
from .common import utc,save
class Entry(ctypes.Structure):
    _fields_=[('dwSize',wintypes.DWORD),('cntUsage',wintypes.DWORD),('th32ProcessID',wintypes.DWORD),('th32DefaultHeapID',ctypes.c_size_t),('th32ModuleID',wintypes.DWORD),('cntThreads',wintypes.DWORD),('th32ParentProcessID',wintypes.DWORD),('pcPriClassBase',wintypes.LONG),('dwFlags',wintypes.DWORD),('szExeFile',wintypes.WCHAR*260)]
def snapshot():
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.CreateToolhelp32Snapshot.argtypes=[wintypes.DWORD,wintypes.DWORD];k.CreateToolhelp32Snapshot.restype=wintypes.HANDLE
    k.Process32FirstW.argtypes=k.Process32NextW.argtypes=[wintypes.HANDLE,ctypes.POINTER(Entry)]
    k.CloseHandle.argtypes=[wintypes.HANDLE]
    h=k.CreateToolhelp32Snapshot(2,0)
    if h==wintypes.HANDLE(-1).value:raise ctypes.WinError()
    rows=[]
    try:
        e=Entry();e.dwSize=ctypes.sizeof(e);ok=k.Process32FirstW(h,ctypes.byref(e))
        while ok:
            rows.append({'pid':e.th32ProcessID,'parent_pid':e.th32ParentProcessID,'name':e.szExeFile})
            ok=k.Process32NextW(h,ctypes.byref(e))
    finally:k.CloseHandle(h)
    return rows
def observe(root_pid,known):
    # Called only while the exact Popen worker is alive.
    rows=snapshot();parents={root_pid};changed=True
    while changed:
        changed=False
        for r in rows:
            if r['parent_pid'] in parents and r['pid'] not in parents:
                parents.add(r['pid']);changed=True
                try:current=identity(r['pid'])
                except OSError:current=None
                if current:known[(r['pid'],current['create_time_100ns'])]={**r,'identity':current,'first_seen':utc()}
def finish(known,out):
    history=[];live=[]
    for r in known.values():
        current=identity(r['pid'])
        if current and current==r['identity']:live.append(r)
        else:history.append({**r,'cleanup':'ALREADY_EXITED'})
    deadline=time.monotonic()+30
    while live and time.monotonic()<deadline:
        time.sleep(.25)
        for r in list(live):
            current=identity(r['pid'])
            if current!=r['identity']:
                live.remove(r);history.append({**r,'cleanup':'EXITED_AFTER_OWNER'})
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];k.OpenProcess.restype=wintypes.HANDLE
    k.TerminateProcess.argtypes=[wintypes.HANDLE,wintypes.UINT];k.CloseHandle.argtypes=[wintypes.HANDLE]
    for r in live:
        current=identity(r['pid'])
        if current!=r['identity']:continue
        h=k.OpenProcess(0x1001,False,r['pid'])
        if not h:raise ctypes.WinError()
        try:
            if identity(r['pid'])!=r['identity']:raise RuntimeError('Process identity changed before cleanup')
            if not k.TerminateProcess(h,77):raise ctypes.WinError()
        finally:k.CloseHandle(h)
        history.append({**r,'cleanup':'STOPPED_OWNED_DESCENDANT_AFTER_OWNER_EXIT','at':utc(),'scope':'Resource cleanup, not an experimental crash intervention'})
    save(out/'OWNED_RESOURCES.json',{'at':utc(),'observed_descendants':history,'unrelated_resources_touched':False})
    return history
