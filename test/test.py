# -*- coding: utf-8 -*-

class A:
    def __init__(self):
        self.x=10
        self.y=[]

class B:
    def __init__(self):
        self.a=20
        
    def foo(self, Aobj):
        Aobj.y.append(26)
        Aobj.y.append(30)

aobj = A()
bobj = B()
bobj.foo(aobj)