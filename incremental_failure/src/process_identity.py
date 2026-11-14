"""Windows process identity without optional packages or PID-only claims."""
import ctypes, os
from ctypes import wintypes

def identity(pid):
    if not isinstance(pid,int) or pid<=0:return None
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
    kernel.OpenProcess.restype=wintypes.HANDLE
    kernel.CloseHandle.argtypes=[wintypes.HANDLE]
    handle=kernel.OpenProcess(0x1000,False,pid)
    if not handle:
        if ctypes.get_last_error()==87:return None
        raise OSError(ctypes.get_last_error(),'Cannot determine process identity')
    try:
        code=wintypes.DWORD()
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
        if not kernel.GetExitCodeProcess(handle,ctypes.byref(code)):raise ctypes.WinError()
        if code.value!=259:return None
        times=[wintypes.FILETIME() for _ in range(4)]
        kernel.GetProcessTimes.argtypes=[wintypes.HANDLE]+[ctypes.POINTER(wintypes.FILETIME)]*4
        if not kernel.GetProcessTimes(handle,*map(ctypes.byref,times)):raise ctypes.WinError()
        buf=ctypes.create_unicode_buffer(32768);size=wintypes.DWORD(len(buf))
        kernel.QueryFullProcessImageNameW.argtypes=[wintypes.HANDLE,wintypes.DWORD,wintypes.LPWSTR,ctypes.POINTER(wintypes.DWORD)]
        if not kernel.QueryFullProcessImageNameW(handle,0,buf,ctypes.byref(size)):raise ctypes.WinError()
        created=(times[0].dwHighDateTime<<32)|times[0].dwLowDateTime
        return {'pid':pid,'create_time_100ns':created,'image':buf.value}
    finally:kernel.CloseHandle(handle)
