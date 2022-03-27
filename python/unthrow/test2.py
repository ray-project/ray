import unthrow
import time
import random
import dis
from collections import deque

class MaxFilter():
    """Filter which returns the maximum value within the last block_size measurements"""
    def __init__(self,block_size=10):
        self.block_size = block_size
        self.history = deque(maxlen=block_size)
    def on_value(self,value):
        self.history.append(value)
        return max(self.history)

class MinFilter():
    """Filter which returns the minimum value within the last block_size measurements"""
    def __init__(self,block_size=10):
        self.block_size = block_size
        self.history = deque(maxlen=block_size)
    def on_value(self,value):
        self.history.append(value)
        return min(self.history)
    
        
class GraphManager():
    def __init__(self,graph,colours={"ax":"rgb(255,0,0)","ay":"rgb(0,255,0)","az":"rgb(0,0,255)"},yMin=-30,yMax=30):
        """Create graphmanager object, resets graph and colour schemes"""
        self.graphs=graph
        self.yScale = [yMin,yMax]
#        for g in self.graphs:
#            if g in colours.keys():
#                graphs.set_style(g,colours[g],self.yScale[0],self.yScale[1])
#            else:
#                graphs.set_style(g,self.getCol(g),self.yScale[0],self.yScale[1])
#        for i in range(100):
#            for g in self.graphs:
#                graphs.on_value(g,0)
    def getCol(self,g):
        """Generate a random colour for each variable"""
        col = "rgb({0},{1},{2})"
        vals = []
        for i in range(3):
            vals.append(int(hash(g)/(i+1))%255)# hashing is not actually consistent
        return col.format(vals[0],vals[1]//2,vals[2])
    def update(self,stretchx=1):
        """Update variable states to graph"""
        unthrow.stop("WOO")
        vars = globals()
        for i in range(stretchx):
            for g in self.graphs:
                print(g,vars[g])
#                graphs.on_value(g,vars[g])

# Create graph manager
gm = GraphManager(["am","test","test_thresh"],yMin=-20,yMax=30)

# Accelerometer Magnitude Filter
amf = MaxFilter(block_size=10)
# Test Metric Filters
testf = MaxFilter(block_size=10)
testf2 = MaxFilter(block_size=10)
testf_max = MaxFilter(block_size=10)
testf2_max = MaxFilter(block_size=10)

# Detection Params
am_upThresh = 18
am_greatThresh = 24
am_downThresh = 14
test_thresh = 5
punching = False

good_punches = 0
great_punches = 0

ax,ay,az,am=0,0,0,0
test=0

print("Start punching")
def mainLoop():
    global punching,am,ax,ay,az,test,gm
    while True:
        u=gm.update
        u(stretchx=1)
        print(".")
        # Sleep
        #time.sleep(0.1)
        
        # Get Data
        ax,ay,az,am = 1,0,0,1

        # Filter Accelerometer Magnitude
        am = amf.on_value(am)

        # High test score indicates legitimate punches
        test = az - ax
        test = testf.on_value(test)
        test = testf_max.on_value(test)
        
        # High test2 score indicates cheating
        test2 = az-ay
        test2 = testf2.on_value(test2)
        test2 = testf2_max.on_value(test2)
        
        # Aggregate into final score - cheating detection metric
        test = test2-test

        # Freeze punch counter if suspected cheating
        if test < test_thresh:
            
            # Toggle State
            if (am > am_upThresh and not punching):# Trigger event
                # Punch is great if acceleration is high
                if am > am_greatThresh:
                    print("Great Punch!")
                    great_punches += 1
                # Punch meets minimum criteria at this acceleration
                else:
                    print("Good Punch!")
                    good_punches += 1
                print("=================")
                print("Great Punches: ",great_punches)
                print("Good Punches: ",good_punches)
                punching = True
            elif (am < am_downThresh and punching):# Trigger release
                punching = False
        
        def kwFn(sx=1):
            unthrow.stop({sx})
#        kwFn(sx=4)
        # Graph
#        with open("test2.py","r") as f:

print(dis.dis(mainLoop))

r=unthrow.Resumer()
while True:
    r.set_interrupt_frequency(2000)
    r.run_once(mainLoop,[])
    print("IN")
#    break
