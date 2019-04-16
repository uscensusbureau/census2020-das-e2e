import time

class stopwatch:
    def __init__(self):
        self.running = False

    def start(self):
        self.t0 = time.time()
        self.running = True
        return self

    def stop(self):
        self.t1 = time.time()
        self.running = False
        return self

    def elapsed(self):
        if self.running:
            return time.time() - self.t0
        else:
            return self.t1 - self.t0


