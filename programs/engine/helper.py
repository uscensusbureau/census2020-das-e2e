import socket
import psutil

###
## Miscellaneous helper methods.
###

gbDenominator = 1024.**3.

def getIP():
    """
    Get the IP address of the current node.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

def deepPMem(pid):
    """
    Get physical memory use of process with given pid,
    as well as physical memory use of all its subprocesses.
    """
    proc = psutil.Process(pid)
    pmem = proc.memory_info().rss
    print("parent process: ", proc.name(), " has child processes...")
    for child in proc.children(recursive=True):
         pmem += child.memory_info().rss
         print(child.name())
    return pmem

class memStats:
    def __init__(self, pid):
        self.pMem = deepPMem(pid) / gbDenominator
        self.sysPMem_tot = psutil.virtual_memory().total / gbDenominator
        self.sysPMem_avail = psutil.virtual_memory().available / gbDenominator

    def __repr__(self):
        return f"(in GB) Total PMem: {self.sysPMem_tot} Available: {self.sysPMem_avail} Process: {self.pMem}"
