import unthrow
my_global=0

def test_iter(maxval):
    for c in range(maxval):
        print("TI:",c)
        yield c

def runtest():
    global my_global
    for c in range(100):
      print("T:",c,my_global)
      my_global+=1
      print("STORED:",my_global)
    print("Done testmod")
