import time
from thespian.actors import *

import sys


class Greeting(object):
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return self.message


class Hello(Actor):
    def receiveMessage(self, message, sender):
        if message == 'hi':
            greeting = Greeting('Hello')
            world = self.createActor(World)
            punct = self.createActor(Punctuate)
            greeting.sendTo = [punct, sender]
            self.send(world, greeting)
        elif isinstance(message, ActorExitRequest):
            print("received ActorExitRequest")


class World(Actor):
    def receiveMessage(self, message, sender):
        if isinstance(message, Greeting):
            message.message = message.message + ", World"
            nextTo = message.sendTo.pop(0)
            self.send(nextTo, message)


class Punctuate(Actor):
    def receiveMessage(self, message, sender):
        if isinstance(message, Greeting):
            message.message = message.message + "!"
            nextTo = message.sendTo.pop(0)
            self.send(nextTo, message)


if __name__ == "__main__":
    actorsys = ActorSystem('multiprocQueueBase')
    hello = actorsys.createActor(Hello)
    print(actorsys.ask(hello, 'hi', 0.00000001))
    print(actorsys.ask(hello, 'hi', 0.00000001))
    print(actorsys.ask(hello, 'hi', 0.2))
    print(actorsys.ask(hello, 'hi', 0.2))
    actorsys.tell(hello, ActorExitRequest())
    time.sleep(1)
    print(actorsys.ask(hello, 'him', 0.2))   # will print "Hello, World!", that should be error
    print(actorsys.ask(hello, 'him', 0.2))
    print(actorsys.ask(hello, 'hi', 0.2))
    print(actorsys.ask(hello, 'hi', 0.2))
    actorsys.shutdown()
