from flask import Flask, request
import pickle
import pyarrow

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        data = request.files['data'].read()
        ctx = pyarrow.default_serialization_context()

        print(ctx.deserialize(data))
        print(pickle.loads(request.files['meta'].read()))
        return '''
        hi!
        '''
    else:
        return '''
        <html><body><h1>hi!</h1></body></html>
        '''

if __name__ == '__main__':
    app.run(debug=True)
