var jb = require('jbinary');
var stream = require('stream');

var task_argument = {
  'jBinary.littleEndian': true,
  is_ref: ['enum', 'int8', [true, false]],
  padding: ['array', 'int8', 7],
  reference: ['if', 'is_ref', {object_id: ['array', 'uint8', 20], padding: ['array', 'int8', 4]}],
  value: ['if_not', 'is_ref', {offset: 'int64', length: 'int64', padding: ['array', 'int8', 8]}]
};

var task_spec_header = {
  'jBinary.littleEndian': true,
  function_id: ['array', 'uint8', 20],
  padding: ['array', 'uint8', 4],
  num_args: ['int64'],
  arg_index: ['int64'],
  num_returns: ['int64'],
  args_value_size: ['int64'],
  args_value_offset: ['int64'],
  arguments: ['array', task_argument, 'num_args']
};

var task_instance = {
  'jBinary.littleEndian': true,
  task_iid: ['array', 'uint8', 20],
  state: 'int32',
  node_id: ['array', 'uint8', 20],
  padding: ['array', 'uint8', 4],
  task_spec_header: ['object', task_spec_header],
};



// Convert string or byte buffer of an object ID to hex string.
function id_to_hex(id) {
  return new Buffer(id).toString('hex');
}

module.exports = {
  parse_task_instance: function (buffer) {
    var binary = new jb(buffer, task_instance);
    binary.read('padding');
    var task_spec = binary.read('task_spec_header');
    var arguments = [];
    for (var i = 0; i < task_spec['arguments'].length; i++) {
      var arg = task_spec['arguments'][i];
      if (arg['is_ref']) {
        console.log(arg['reference']['object_id'])
        arguments.push(id_to_hex(arg['reference']['object_id']));
      } else {
        arguments.push("value");
      }
    }
    var state = binary.read('state');
    var node_id = binary.read('node_id');
    return {
      state: state,
      node_id: id_to_hex(node_id),
      function_id: id_to_hex(task_spec['function_id']),
      arguments: arguments
    }
  }
}
