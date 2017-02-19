RecentTasks = function(all_recent_tasks_elem) {
  var self = this;

  var verticalPadding = 10;
  var barHeight = 25;

  var all_recent_tasks_div = d3.select(all_recent_tasks_elem);

  this.generate_task_info = function(d) {
    return "<div><b>Total Time:</b> " + d.task_formatted_time + "</div>" +
           "<div><b>Time Getting Arguments:</b> " + d.get_arguments_formatted_time + "</div>" +
           "<div><b>Time in Execution:</b> " + d.execute_formatted_time + "</div>" +
           "<div><b>Time Storing Outputs:</b> " + d.store_outputs_formatted_time + "</div>";
  }

  this.draw_new_node_tasks = function(all_task_info, task_info, width, svg, info) {
    var height = task_info.num_workers * barHeight + 2 * verticalPadding;

    svg.attr("width", width)
       .attr("height", height);

    var borderPath = svg.append("rect")
        .attr("x", 0)
        .attr("y", 0)
        .attr("height", height)
        .attr("width", width)
        .style("stroke", "black")
        .style("fill", "none")
        .style("stroke-width", 1);

    var x = d3.scaleLinear()
        .domain([all_task_info.min_time, all_task_info.max_time])
        .range([-1, width + 1]);

    var task_rects = svg.append("g").attr("class", "task_rects");
    var get_arguments_rects = svg.append("g").attr("class", "get_arguments_rects");
    var execute_rects = svg.append("g").attr("class", "execute_rects");
    var store_outputs_rects = svg.append("g").attr("class", "store_outputs_rects");

    task_rects.selectAll("rect")
        .data(task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.task[0]); })
        .attr("y", function (d) { return verticalPadding + d.worker_index * barHeight; })
        .attr("width", function (d) { return x(d.task[1]) - x(d.task[0]); })
        .attr("height", function (d) { return barHeight - 1; })
        .attr("fill", "orange")
        .attr("id", function (d) { d.store_outputs[1]; })
        .on("click", function(d, i) {
          info.html(self.generate_task_info(d));
        })

    get_arguments_rects.selectAll("rect")
        .data(task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.get_arguments[0]); })
        .attr("y", function (d) { return verticalPadding + d.worker_index * barHeight + 1; })
        .attr("width", function (d) { return x(d.get_arguments[1]) - x(d.get_arguments[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "black")
        .on("click", function(d, i) {
          info.html(self.generate_task_info(d));
        })

    execute_rects.selectAll("rect")
        .data(task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.execute[0]); })
        .attr("y", function (d) { return verticalPadding + d.worker_index * barHeight + 1; })
        .attr("width", function (d) { return x(d.execute[1]) - x(d.execute[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "blue")
        .on("click", function(d, i) {
          info.html(self.generate_task_info(d));
        })

    store_outputs_rects.selectAll("rect")
        .data(task_info.task_data)
      .enter()
      .append("rect")
        .attr("x", function (d) { return x(d.store_outputs[0]); })
        .attr("y", function (d) { return verticalPadding + d.worker_index * barHeight + 1; })
        .attr("width", function (d) { return x(d.store_outputs[1]) - x(d.store_outputs[0]); })
        .attr("height", function (d) { return barHeight - 3; })
        .attr("fill", "green")
        .on("click", function(d, i) {
          info.html(self.generate_task_info(d));
        })
  }

  this.draw_new_tasks = function(all_task_info, width) {
    // Call draw_new_node_tasks once for each node.
    for (i = 0; i < all_task_info.task_data.length; i++) {
      var new_svg = all_recent_tasks_div.append("svg");
      var info = all_recent_tasks_div.append("div");
      this.draw_new_node_tasks(all_task_info, all_task_info.task_data[i], width, new_svg, info);
    }
  }

  this.erase = function() {
    all_recent_tasks_div.selectAll("svg").remove();
    all_recent_tasks_div.selectAll("div").remove();
  }
}
