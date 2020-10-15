from misc import fibs
from misc import xrange
from misc import circular
from misc import fibonacci

a = 20

def fib():
    a, b = 0, 1
    while 1:
        yield a
        a, b = b, a + b

b = fib()
next(b)

__fibs__ = []
for i in xrange(a):
    __fibs__.append(next(b))

normalize = lambda foo:[str(f) for f in foo]

print(', '.join(normalize(__fibs__)))

b = fibonacci()
next(b)

fibs2 = []
for i in xrange(a):
    fibs2.append(next(b))

print(', '.join(normalize(fibs2)))

fibs3 = []
b = fibonacci()
while (1):
    f = next(b)
    if (f > 7):
        fibs3.append(f)
        if (len(fibs3) == 8):
            break

print(len(fibs3))
print(', '.join(normalize(fibs3)))

c = fibs(n=7, items=8)

for i in xrange(100):
    print(next(c))