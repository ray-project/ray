function collapse_experiment_list() {
  $("#sidebar").toggleClass("collapsed");
  $("#content").toggleClass("col-md-8");
  $(".collapser").toggleClass("fa-chevron-left fa-chevron-right");
  var over_flow_attr = $(".experiment-list-container").css("overflow-y");
  if (over_flow_attr == "scroll") {
    $(".experiment-list-container").css("overflow-y", "visible")
  } else {
    $(".experiment-list-container").css("overflow-y", "scroll")
  }
}
