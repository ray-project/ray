$.ajaxSetup({async: false});

$(document)
    .ready(function() {
      $('#jobs_info_table')
          .DataTable({
            "bPaginate": true,
            "bLengthChange": true,
            "bFilter": true,
            "bInfo": true,
            "bSort": false,
            "bAutoWidth": false,
            "order": [[4, "desc"]]
          });
      $('#trials_info_table')
          .DataTable({
            "bPaginate": true,
            "bLengthChange": true,
            "bFilter": true,
            "bInfo": true,
            "bSort": false,
            "bAutoWidth": false,
            "order": [[5, "desc"]]
          });
      $('#metrics_info_table')
          .DataTable({
            "bPaginate": true,
            "bLengthChange": true,
            "bFilter": true,
            "bInfo": true,
            "bSort": true,
            "bAutoWidth": false
          });
    });
