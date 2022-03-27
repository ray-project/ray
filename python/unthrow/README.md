# Unthrow package for python 3.8

This defines a function stop(message), which saves the stack state and then throws an unthrow.ResumableException.

ResumableException has a method run_once() which runs a main loop function, and if it was previously stopped, restores the stack state to where stop was called. It should work with loops, with statements and other block things. It may or may not work with call stacks that include C code...

This allows you to stop and start the interpreter from deep in the stack, to do stuff in javascript or whatever is running the interpreter..

3.9 stores stack level as a number not a pointer, it will need this minor change to work on 3.9

To make it work:

1) Create a resumer object (at the level you want interrupts and the like to happen).
2) If you want regular interrupts as well as deliberately thrown ones, then call `resumer.set_interrupt_frequency(line_count)`, which throws an interrupt every line_count lines of python execution.
3) Then just repeatedly call run_once with your main function (the thing you want to be resuming from) until it is finished (`resumer.finished==True`). It will set finished when the main function returns normally (as opposed to an interrupt or a call to `unthrow.stop` making it fall out.

```python
import unthrow
# this is an arbitrary argument that will
# be passed to main application, e.g. a state object
__appArg="WOO"
r=unthrow.Resumer()
# throw an interrupt every 2 lines of execution
r.set_interrupt_frequency(2)
while r.finished==False:
    r.run_once(mainApp,__appArg)
    # do anything that is supposed to happen outside the main loop here
    # like handle input or whatever
```

If you're using it in pyodide and you want interrupts to drop right out to javascript, you can call run_once from javascript and use settimeout to allow you to handle js messages or whatever. Something roughly like this:

```javascript
function init()
{
  pyodide.runPython(`
__appArg="WOO" 
import unthrow
__resumer=unthrow.Resumer()`)
# interrupt and handle js stuff every 50 lines of code
r.set_interrupt_frequency(50);
}

function doLoop()
{
  pyodide.runPython(`
__resumer.run_once(mainApp, __appArg)
`)
  setTimeout(doLoop,0);
}
```

In your main loop, where you want to drop out in a resumable way, you can call unthrow.stop, with a parameter which will turn up in the resumer object's `resume_params` member.

e.g. My time.sleep implementation looks like this:
```
import time
time.sleep=lambda t: unthrow.stop({"cmd":"sleep","time":t})
```

Then in the javascript, it looks like this; i.e. `time.sleep` throws an unthrowable exception, which then goes back out to javascript which does a settimeout before the next loop iteration runs:
```
function doLoop()
{
  let retVal=pyodide.runPython(`__resumer.run_once(mainApp, __appArg)`)
  let result=retVal.toJs();
  if(result.cmd && result.cmd=='sleep')
  {
    setTimeout(doLoop,result.time*1000);
  }else
  {
    setTimeout(doLoop,0);
  }
}
```
