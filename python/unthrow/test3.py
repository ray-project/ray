import unthrow

def app(x):
    exec("for c in range(100):\n  print(c)",globals(),{})

r=unthrow.Resumer()
r.set_interrupt_frequency(2)
while r.finished==False:
    print("@",end="")
    r.run_once(app,[None])
