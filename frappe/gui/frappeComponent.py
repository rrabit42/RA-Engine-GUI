from frappeController import FrappeController


class FrappeComponent:
    controller: FrappeController = None

    def __init__(self, controller: FrappeController):
        self.controller = controller

