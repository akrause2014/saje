from __future__ import print_function

class StatusReporter(object):
    """Your subclass constructor must set verbosity
    """
    def critical(self, *args):
        print(*args)
        return
    
    def info(self, *args):
        if self.verbosity >= 1:
            print(*args)
        return
    
    def debug(self, *args):
        if self.verbosity >= 2:
            print(*args)
        return
