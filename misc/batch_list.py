import threading

class BatchedList():
    def __init__(self, batchSize):
        self.batchSize = batchSize
        self.lock = threading.Lock()
        self.items = []
        self.history = []

    def add(self, item):
        with self.lock:
            if item in self.items or item in self.history:
                return

            self.items.append(item)

    def contained(self, item):
        with self.lock:
            return item in self.history

    def contains(self, item):
        with self.lock:
            return item in self.items

    def getBatch(self, only_full=True):
        with self.lock:
            if len(self.items) >= self.batchSize:
                items, self.items = self.items[0:self.batchSize], self.items[self.batchSize:]
            elif len(self.items) > 0 and not only_full:
                items, self.items = self.items, []
            else:
                items = None

            if items is not None:
                self.history += items

            return items

    def count(self):
        with self.lock:
            return len(self.items)
