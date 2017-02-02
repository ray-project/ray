RecentTasks = function(elem, options) {
  var self = this;

  this.options = options;
  var barHeight = 25;

  var svg = d3.select(elem)
      .attr("width", this.options.width)
      .attr("height", this.options.height);

  this.draw_new_tasks = function(task_info) {
    this.task_info = task_info;
    var x = d3.scaleLinear()
        .domain([task_info.min_time, task_info.max_time])
        .range([-1, width + 1]);

    var xAxis = d3.axisBottom(x)
        .tickSize(-height);

    var gx = svg.append("g")
        .attr("class", "axis axis--x")
        .attr("transform", "translate(0," + (height - 10) + ")")
        .call(xAxis);

    var task_rects = svg.append("g").attr("class", "task_rects");
    var get_arguments_rects = svg.append("g").attr("class", "get_arguments_rects");
    var execute_rects = svg.append("g").attr("class", "execute_rects");
    var store_outputs_rects = svg.append("g").attr("class", "store_outputs_rects");

    task_rects.selectAll("rect")
        .data(this.task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.task[0]); })
        .attr("y", function (d) { return (d.worker_index + 1) * barHeight; })
        .attr("width", function (d) { return x(d.task[1]) - x(d.task[0]); })
        .attr("height", function (d) { return barHeight - 1; })
        .attr("fill", "orange")

    get_arguments_rects.selectAll("rect")
        .data(this.task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.get_arguments[0]); })
        .attr("y", function (d) { return (d.worker_index + 1) * barHeight + 1; })
        .attr("width", function (d) { return x(d.get_arguments[1]) - x(d.get_arguments[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "black")

    execute_rects.selectAll("rect")
        .data(this.task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.execute[0]); })
        .attr("y", function (d) { return (d.worker_index + 1) * barHeight + 1; })
        .attr("width", function (d) { return x(d.execute[1]) - x(d.execute[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "blue")

    store_outputs_rects.selectAll("rect")
        .data(this.task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.store_outputs[0]); })
        .attr("y", function (d) { return (d.worker_index + 1) * barHeight + 1; })
        .attr("width", function (d) { return x(d.store_outputs[1]) - x(d.store_outputs[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "green")
  }

  this.erase = function() {
    svg.selectAll("g").remove()
  }
}
