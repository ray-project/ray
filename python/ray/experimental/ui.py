import ipywidgets as widgets
import numpy as np
import os
import pprint
import ray
import shutil
import tempfile
import time

from IPython.display import display, IFrame, clear_output

# Instances of this class maintains keep track of whether or not a
# callback is currently executing. Since the execution of the callback
# may trigger more calls to the callback, this is used to prevent infinite
# recursions.


class _EventRecursionContextManager(object):
    def __init__(self):
        self.should_recurse = True

    def __enter__(self):
        self.should_recurse = False

    def __exit__(self, *args):
        self.should_recurse = True


total_time_value = "% total time"
total_tasks_value = "% total tasks"

# Function that returns instances of sliders and handles associated events.


def get_sliders(update):
    # Start_box value indicates the desired start point of queried window.
    start_box = widgets.FloatText(
        description="Start Time:",
        disabled=True,
    )

    # End_box value indicates the desired end point of queried window.
    end_box = widgets.FloatText(
        description="End Time:",
        disabled=True,
    )

    # Percentage slider. Indicates either % of total time or total tasks
    # depending on what breakdown_opt is set to.
    range_slider = widgets.IntRangeSlider(
        value=[0, 100],
        min=0,
        max=100,
        step=1,
        description="%:",
        continuous_update=False,
        orientation="horizontal",
        readout=True,
    )

    # Indicates the number of tasks that the user wants to be returned. Is
    # disabled when the breakdown_opt value is set to total_time_value.
    num_tasks_box = widgets.IntText(description="Num Tasks:", disabled=False)

    # Dropdown bar that lets the user choose between modifying % of total
    # time or total number of tasks.
    breakdown_opt = widgets.Dropdown(
        options=[total_time_value, total_tasks_value],
        value=total_tasks_value,
        description="Selection Options:")

    # Display box for layout.
    total_time_box = widgets.VBox([start_box, end_box])

    # This sets the CSS style display to hide the box.
    total_time_box.layout.display = 'none'

    # Initially passed in to the update_wrapper function.
    INIT_EVENT = "INIT"

    # Create instance of context manager to determine whether callback is
    # currently executing
    out_recursion = _EventRecursionContextManager()

    def update_wrapper(event):
        # Feature received a callback, but it shouldn't be executed
        # because the callback was the result of a different feature
        # executing its callback based on user input.
        if not out_recursion.should_recurse:
            return

        # Feature received a callback and it should be executed because
        # the callback was the result of user input.
        with out_recursion:
            smallest, largest, num_tasks = ray.global_state._job_length()
            diff = largest - smallest
            if num_tasks is not 0:

                # Describes the initial values that the slider/text box
                # values should be set to.
                if event == INIT_EVENT:
                    if breakdown_opt.value == total_tasks_value:
                        num_tasks_box.value = -min(10000, num_tasks)
                        range_slider.value = (int(
                            100 - (100. * -num_tasks_box.value) / num_tasks),
                                              100)
                    else:
                        low, high = map(lambda x: x / 100., range_slider.value)
                        start_box.value = round(diff * low, 2)
                        end_box.value = round(diff * high, 2)

                # Event was triggered by a change in the start_box value.
                elif event["owner"] == start_box:
                    if start_box.value > end_box.value:
                        start_box.value = end_box.value
                    elif start_box.value < 0:
                        start_box.value = 0
                    low, high = range_slider.value
                    range_slider.value = (int((start_box.value * 100.) / diff),
                                          high)

                # Event was triggered by a change in the end_box value.
                elif event["owner"] == end_box:
                    if start_box.value > end_box.value:
                        end_box.value = start_box.value
                    elif end_box.value > diff:
                        end_box.value = diff
                    low, high = range_slider.value
                    range_slider.value = (low,
                                          int((end_box.value * 100.) / diff))

                # Event was triggered by a change in the breakdown options
                # toggle.
                elif event["owner"] == breakdown_opt:
                    if breakdown_opt.value == total_tasks_value:
                        start_box.disabled = True
                        end_box.disabled = True
                        num_tasks_box.disabled = False
                        total_time_box.layout.display = 'none'

                        # Make CSS display go back to the default settings.
                        num_tasks_box.layout.display = None
                        num_tasks_box.value = min(10000, num_tasks)
                        range_slider.value = (int(
                            100 - (100. * num_tasks_box.value) / num_tasks),
                                              100)
                    else:
                        start_box.disabled = False
                        end_box.disabled = False
                        num_tasks_box.disabled = True

                        # Make CSS display go back to the default settings.
                        total_time_box.layout.display = None
                        num_tasks_box.layout.display = 'none'
                        range_slider.value = (
                            int((start_box.value * 100.) / diff),
                            int((end_box.value * 100.) / diff))

                # Event was triggered by a change in the range_slider
                # value.
                elif event["owner"] == range_slider:
                    low, high = map(lambda x: x / 100., range_slider.value)
                    if breakdown_opt.value == total_tasks_value:
                        old_low, old_high = event["old"]
                        new_low, new_high = event["new"]
                        if old_low != new_low:
                            range_slider.value = (new_low, 100)
                            num_tasks_box.value = (
                                -(100. - new_low) / 100. * num_tasks)
                        else:
                            range_slider.value = (0, new_high)
                            num_tasks_box.value = new_high / 100. * num_tasks
                    else:
                        start_box.value = round(diff * low, 2)
                        end_box.value = round(diff * high, 2)

                # Event was triggered by a change in the num_tasks_box
                # value.
                elif event["owner"] == num_tasks_box:
                    if num_tasks_box.value > 0:
                        range_slider.value = (
                            0, int(
                                100 * float(num_tasks_box.value) / num_tasks))
                    elif num_tasks_box.value < 0:
                        range_slider.value = (100 + int(
                            100 * float(num_tasks_box.value) / num_tasks), 100)

                if not update:
                    return

                diff = largest - smallest

                # Low and high are used to scale the times that are
                # queried to be relative to the absolute time.
                low, high = map(lambda x: x / 100., range_slider.value)

                # Queries to task_profiles based on the slider and text
                # box values.
                # (Querying based on the % total amount of time.)
                if breakdown_opt.value == total_time_value:
                    tasks = _truncated_task_profiles(
                        start=(smallest + diff * low),
                        end=(smallest + diff * high))

                # (Querying based on % of total number of tasks that were
                # run.)
                elif breakdown_opt.value == total_tasks_value:
                    if range_slider.value[0] == 0:
                        tasks = _truncated_task_profiles(
                            num_tasks=(int(num_tasks * high)), fwd=True)
                    else:
                        tasks = _truncated_task_profiles(
                            num_tasks=(int(num_tasks * (high - low))),
                            fwd=False)

                update(smallest, largest, num_tasks, tasks)

    # Get updated values from a slider or text box, and update the rest of
    # them accordingly.
    range_slider.observe(update_wrapper, names="value")
    breakdown_opt.observe(update_wrapper, names="value")
    start_box.observe(update_wrapper, names="value")
    end_box.observe(update_wrapper, names="value")
    num_tasks_box.observe(update_wrapper, names="value")

    # Initializes the sliders
    update_wrapper(INIT_EVENT)

    # Display sliders and search boxes
    display(breakdown_opt,
            widgets.HBox([range_slider, total_time_box, num_tasks_box]))

    # Return the sliders and text boxes
    return start_box, end_box, range_slider, breakdown_opt


def object_search_bar():
    object_search = widgets.Text(
        value="",
        placeholder="Object ID",
        description="Search for an object:",
        disabled=False)
    display(object_search)

    def handle_submit(sender):
        pp = pprint.PrettyPrinter()
        pp.pprint(ray.global_state.object_table(object_search.value))

    object_search.on_submit(handle_submit)


def task_search_bar():
    task_search = widgets.Text(
        value="",
        placeholder="Task ID",
        description="Search for a task:",
        disabled=False)
    display(task_search)

    def handle_submit(sender):
        pp = pprint.PrettyPrinter()
        pp.pprint(ray.global_state.task_table(task_search.value))

    task_search.on_submit(handle_submit)


# Hard limit on the number of tasks to return to the UI client at once
MAX_TASKS_TO_VISUALIZE = 10000


# Wrapper that enforces a limit on the number of tasks to visualize
def _truncated_task_profiles(start=None, end=None, num_tasks=None, fwd=True):
    if num_tasks is None:
        num_tasks = MAX_TASKS_TO_VISUALIZE
        print("Warning: at most {} tasks will be fetched within this "
              "time range.".format(MAX_TASKS_TO_VISUALIZE))
    elif num_tasks > MAX_TASKS_TO_VISUALIZE:
        print("Warning: too many tasks to visualize, "
              "fetching only the first {} of {}.".format(
                  MAX_TASKS_TO_VISUALIZE, num_tasks))
        num_tasks = MAX_TASKS_TO_VISUALIZE
    return ray.global_state.task_profiles(num_tasks, start, end, fwd)


# Helper function that guarantees unique and writeable temp files.
# Prevents clashes in task trace files when multiple notebooks are running.
def _get_temp_file_path(**kwargs):
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, dir=os.getcwd(), **kwargs)
    temp_file_path = temp_file.name
    temp_file.close()
    return os.path.relpath(temp_file_path)


def task_timeline():
    path_input = widgets.Button(description="View task timeline")

    breakdown_basic = "Basic"
    breakdown_task = "Task Breakdowns"

    breakdown_opt = widgets.Dropdown(
        options=["Basic", "Task Breakdowns"],
        value="Task Breakdowns",
        disabled=False,
    )
    obj_dep = widgets.Checkbox(
        value=True, disabled=False, layout=widgets.Layout(width='20px'))
    task_dep = widgets.Checkbox(
        value=True, disabled=False, layout=widgets.Layout(width='20px'))
    # Labels to bypass width limitation for descriptions.
    label_tasks = widgets.Label(
        value='Task submissions', layout=widgets.Layout(width='110px'))
    label_objects = widgets.Label(
        value='Object dependencies', layout=widgets.Layout(width='130px'))
    label_options = widgets.Label(
        value='View options:', layout=widgets.Layout(width='100px'))
    start_box, end_box, range_slider, time_opt = get_sliders(False)
    display(widgets.HBox([task_dep, label_tasks, obj_dep, label_objects]))
    display(widgets.HBox([label_options, breakdown_opt]))
    display(path_input)

    # Check that the trace viewer renderer file is present, and copy it to the
    # current working directory if it is not present.
    if not os.path.exists("trace_viewer_full.html"):
        shutil.copy(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../core/src/catapult_files/trace_viewer_full.html"),
            "trace_viewer_full.html")

    def handle_submit(sender):
        json_tmp = tempfile.mktemp() + ".json"

        # Determine whether task components should be displayed or not.
        if breakdown_opt.value == breakdown_basic:
            breakdown = False
        elif breakdown_opt.value == breakdown_task:
            breakdown = True
        else:
            raise ValueError("Unexpected breakdown value '{}'".format(
                breakdown_opt.value))

        low, high = map(lambda x: x / 100., range_slider.value)

        smallest, largest, num_tasks = ray.global_state._job_length()
        diff = largest - smallest

        if time_opt.value == total_time_value:
            tasks = _truncated_task_profiles(
                start=smallest + diff * low, end=smallest + diff * high)
        elif time_opt.value == total_tasks_value:
            if range_slider.value[0] == 0:
                tasks = _truncated_task_profiles(
                    num_tasks=int(num_tasks * high), fwd=True)
            else:
                tasks = _truncated_task_profiles(
                    num_tasks=int(num_tasks * (high - low)), fwd=False)
        else:
            raise ValueError("Unexpected time value '{}'".format(
                time_opt.value))
        # Write trace to a JSON file
        print("Collected profiles for {} tasks.".format(len(tasks)))
        print("Dumping task profile data to {}, "
              "this might take a while...".format(json_tmp))
        ray.global_state.dump_catapult_trace(
            json_tmp,
            tasks,
            breakdowns=breakdown,
            obj_dep=obj_dep.value,
            task_dep=task_dep.value)
        print("Opening html file in browser...")

        trace_viewer_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../core/src/catapult_files/index.html")

        html_file_path = _get_temp_file_path(suffix=".html")
        json_file_path = _get_temp_file_path(suffix=".json")

        print("Pointing to {} named {}".format(json_tmp, json_file_path))
        shutil.copy(json_tmp, json_file_path)

        with open(trace_viewer_path) as f:
            data = f.read()

        # Replace the demo data path with our own
        # https://github.com/catapult-project/catapult/blob/
        # 33a9271eb3cf5caf925293ec6a4b47c94f1ac968/tracing/bin/index.html#L107
        data = data.replace("../test_data/big_trace.json", json_file_path)

        with open(html_file_path, "w+") as f:
            f.write(data)

        # Display the task trace within the Jupyter notebook
        clear_output(wait=True)
        print("To view fullscreen, open chrome://tracing in Google Chrome "
              "and load `{}`".format(json_tmp))
        display(IFrame(html_file_path, 900, 800))

    path_input.on_click(handle_submit)


def task_completion_time_distribution():
    from bokeh.models import ColumnDataSource
    from bokeh.layouts import gridplot
    from bokeh.plotting import figure, show, helpers
    from bokeh.io import output_notebook, push_notebook
    from bokeh.resources import CDN
    output_notebook(resources=CDN)

    # Create the Bokeh plot
    p = figure(
        title="Task Completion Time Distribution",
        tools=["save", "hover", "wheel_zoom", "box_zoom", "pan"],
        background_fill_color="#FFFFFF",
        x_range=(0, 1),
        y_range=(0, 1))

    # Create the data source that the plot pulls from
    source = ColumnDataSource(data={"top": [], "left": [], "right": []})

    # Plot the histogram rectangles
    p.quad(
        top="top",
        bottom=0,
        left="left",
        right="right",
        source=source,
        fill_color="#B3B3B3",
        line_color="#033649")

    # Label the plot axes
    p.xaxis.axis_label = "Duration in seconds"
    p.yaxis.axis_label = "Number of tasks"

    handle = show(
        gridplot(
            p,
            ncols=1,
            plot_width=500,
            plot_height=500,
            toolbar_location="below"),
        notebook_handle=True)

    # Function to update the plot
    def task_completion_time_update(abs_earliest, abs_latest, abs_num_tasks,
                                    tasks):
        if len(tasks) == 0:
            return

        # Create the distribution to plot
        distr = []
        for task_id, data in tasks.items():
            distr.append(data["store_outputs_end"] -
                         data["get_arguments_start"])

        # Create a histogram from the distribution
        top, bin_edges = np.histogram(distr, bins="auto")
        left = bin_edges[:-1]
        right = bin_edges[1:]

        source.data = {"top": top, "left": left, "right": right}

        # Set the x and y ranges
        x_range = (min(left) if len(left) else 0, max(right)
                   if len(right) else 1)
        y_range = (0, max(top) + 1 if len(top) else 1)

        x_range = helpers._get_range(x_range)
        p.x_range.start = x_range.start
        p.x_range.end = x_range.end

        y_range = helpers._get_range(y_range)
        p.y_range.start = y_range.start
        p.y_range.end = y_range.end

        # Push updates to the plot
        push_notebook(handle=handle)

    get_sliders(task_completion_time_update)


def compute_utilizations(abs_earliest,
                         abs_latest,
                         num_tasks,
                         tasks,
                         num_buckets,
                         use_abs_times=False):
    if len(tasks) == 0:
        return [], [], []

    if use_abs_times:
        earliest_time = abs_earliest
        latest_time = abs_latest
    else:
        # Determine what the earliest and latest tasks are out of the ones
        # that are passed in
        earliest_time = time.time()
        latest_time = 0
        for task_id, data in tasks.items():
            latest_time = max((latest_time, data["store_outputs_end"]))
            earliest_time = min((earliest_time, data["get_arguments_start"]))

    # Add some epsilon to latest_time to ensure that the end time of the
    # last task falls __within__ a bucket, and not on the edge
    latest_time += 1e-6

    # Compute average CPU utilization per time bucket by summing
    # cpu-time per bucket
    bucket_time_length = (latest_time - earliest_time) / float(num_buckets)
    cpu_time = [0 for _ in range(num_buckets)]

    for data in tasks.values():
        task_start_time = data["get_arguments_start"]
        task_end_time = data["store_outputs_end"]

        start_bucket = int(
            (task_start_time - earliest_time) / bucket_time_length)
        end_bucket = int((task_end_time - earliest_time) / bucket_time_length)
        # Walk over each time bucket that this task intersects, adding the
        # amount of time that the task intersects within each bucket
        for bucket_idx in range(start_bucket, end_bucket + 1):
            bucket_start_time = (
                (earliest_time + bucket_idx) * bucket_time_length)
            bucket_end_time = (
                (earliest_time + (bucket_idx + 1)) * bucket_time_length)

            task_start_time_within_bucket = max(task_start_time,
                                                bucket_start_time)
            task_end_time_within_bucket = min(task_end_time, bucket_end_time)
            task_cpu_time_within_bucket = (
                task_end_time_within_bucket - task_start_time_within_bucket)

            if bucket_idx > -1 and bucket_idx < num_buckets:
                cpu_time[bucket_idx] += task_cpu_time_within_bucket

    # Cpu_utilization is the average cpu utilization of the bucket, which
    # is just cpu_time divided by bucket_time_length.
    cpu_utilization = list(
        map(lambda x: x / float(bucket_time_length), cpu_time))

    # Generate histogram bucket edges. Subtract out abs_earliest to get
    # relative time.
    all_edges = [
        earliest_time - abs_earliest + i * bucket_time_length
        for i in range(num_buckets + 1)
    ]
    # Left edges are all but the rightmost edge, right edges are all but
    # the leftmost edge.
    left_edges = all_edges[:-1]
    right_edges = all_edges[1:]

    return left_edges, right_edges, cpu_utilization


def cpu_usage():
    from bokeh.layouts import gridplot
    from bokeh.plotting import figure, show, helpers
    from bokeh.resources import CDN
    from bokeh.io import output_notebook, push_notebook
    from bokeh.models import ColumnDataSource
    output_notebook(resources=CDN)

    # Parse the client table to determine how many CPUs are available
    num_cpus = 0
    client_table = ray.global_state.client_table()
    for node_ip, client_list in client_table.items():
        for client in client_list:
            if "CPU" in client:
                num_cpus += client["CPU"]

    # Update the plot based on the sliders
    def plot_utilization():
        # Create the Bokeh plot
        time_series_fig = figure(
            title="CPU Utilization",
            tools=["save", "hover", "wheel_zoom", "box_zoom", "pan"],
            background_fill_color="#FFFFFF",
            x_range=[0, 1],
            y_range=[0, 1])

        # Create the data source that the plot will pull from
        time_series_source = ColumnDataSource(
            data=dict(left=[], right=[], top=[]))

        # Plot the rectangles representing the distribution
        time_series_fig.quad(
            left="left",
            right="right",
            top="top",
            bottom=0,
            source=time_series_source,
            fill_color="#B3B3B3",
            line_color="#033649")

        # Label the plot axes
        time_series_fig.xaxis.axis_label = "Time in seconds"
        time_series_fig.yaxis.axis_label = "Number of CPUs used"

        handle = show(
            gridplot(
                time_series_fig,
                ncols=1,
                plot_width=500,
                plot_height=500,
                toolbar_location="below"),
            notebook_handle=True)

        def update_plot(abs_earliest, abs_latest, abs_num_tasks, tasks):
            num_buckets = 100
            left, right, top = compute_utilizations(
                abs_earliest, abs_latest, abs_num_tasks, tasks, num_buckets)

            time_series_source.data = {
                "left": left,
                "right": right,
                "top": top
            }

            x_range = (max(0, min(left)) if len(left) else 0, max(right)
                       if len(right) else 1)
            y_range = (0, max(top) + 1 if len(top) else 1)

            # Define the axis ranges
            x_range = helpers._get_range(x_range)
            time_series_fig.x_range.start = x_range.start
            time_series_fig.x_range.end = x_range.end

            y_range = helpers._get_range(y_range)
            time_series_fig.y_range.start = y_range.start
            time_series_fig.y_range.end = num_cpus

            # Push the updated data to the notebook
            push_notebook(handle=handle)

        get_sliders(update_plot)

    plot_utilization()


# Function to create the cluster usage "heat map"
def cluster_usage():
    from bokeh.io import show, output_notebook, push_notebook
    from bokeh.resources import CDN
    from bokeh.plotting import figure
    from bokeh.models import (
        ColumnDataSource,
        HoverTool,
        LinearColorMapper,
        BasicTicker,
        ColorBar,
    )
    output_notebook(resources=CDN)

    # Initial values
    source = ColumnDataSource(
        data={
            "node_ip_address": ['127.0.0.1'],
            "time": ['0.5'],
            "num_tasks": ['1'],
            "length": [1]
        })

    # Define the color schema
    colors = [
        "#75968f", "#a5bab7", "#c9d9d3", "#e2e2e2", "#dfccce", "#ddb7b1",
        "#cc7878", "#933b41", "#550b1d"
    ]
    mapper = LinearColorMapper(palette=colors, low=0, high=2)

    TOOLS = "hover, save, xpan, box_zoom, reset, xwheel_zoom"

    # Create the plot
    p = figure(
        title="Cluster Usage",
        y_range=list(set(source.data['node_ip_address'])),
        x_axis_location="above",
        plot_width=900,
        plot_height=500,
        tools=TOOLS,
        toolbar_location='below')

    # Format the plot axes
    p.grid.grid_line_color = None
    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.axis.major_label_text_font_size = "10pt"
    p.axis.major_label_standoff = 0
    p.xaxis.major_label_orientation = np.pi / 3

    # Plot rectangles
    p.rect(
        x="time",
        y="node_ip_address",
        width="length",
        height=1,
        source=source,
        fill_color={
            "field": "num_tasks",
            "transform": mapper
        },
        line_color=None)

    # Add legend to the side of the plot
    color_bar = ColorBar(
        color_mapper=mapper,
        major_label_text_font_size="8pt",
        ticker=BasicTicker(desired_num_ticks=len(colors)),
        label_standoff=6,
        border_line_color=None,
        location=(0, 0))
    p.add_layout(color_bar, "right")

    # Define hover tool
    p.select_one(HoverTool).tooltips = [("Node IP Address",
                                         "@node_ip_address"),
                                        ("Number of tasks running",
                                         "@num_tasks"), ("Time", "@time")]

    # Define the axis labels
    p.xaxis.axis_label = "Time in seconds"
    p.yaxis.axis_label = "Node IP Address"
    handle = show(p, notebook_handle=True)
    workers = ray.global_state.workers()

    # Function to update the heat map
    def heat_map_update(abs_earliest, abs_latest, abs_num_tasks, tasks):
        if len(tasks) == 0:
            return

        earliest = time.time()
        latest = 0

        node_to_tasks = dict()
        # Determine which task has the earlest start time out of the ones
        # passed into the update function
        for task_id, data in tasks.items():
            if data["score"] > latest:
                latest = data["score"]
            if data["score"] < earliest:
                earliest = data["score"]
            worker_id = data["worker_id"]
            node_ip = workers[worker_id]["node_ip_address"]
            if node_ip not in node_to_tasks:
                node_to_tasks[node_ip] = {}
            node_to_tasks[node_ip][task_id] = data

        nodes = []
        times = []
        lengths = []
        num_tasks = []

        for node_ip, task_dict in node_to_tasks.items():
            left, right, top = compute_utilizations(
                earliest, latest, abs_num_tasks, task_dict, 100, True)
            for (l, r, t) in zip(left, right, top):
                nodes.append(node_ip)
                times.append((l + r) / 2)
                lengths.append(r - l)
                num_tasks.append(t)

        # Set the y range of the plot to be the node IP addresses
        p.y_range.factors = list(set(nodes))

        mapper.low = min(min(num_tasks), 0)
        mapper.high = max(max(num_tasks), 1)

        # Update plot with new data based on slider and text box values
        source.data = {
            "node_ip_address": nodes,
            "time": times,
            "num_tasks": num_tasks,
            "length": lengths
        }

        push_notebook(handle=handle)

    get_sliders(heat_map_update)
