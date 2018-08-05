/**
 * adding sorting ability to HTML tables with Bootstrap styling
 * @summary HTML tables sorting ability
 * @version 2.0.1
 * @requires tinysort, moment.js, jQuery
 * @license MIT
 * @author Matus Brlit (drvic10k)
 * @copyright Matus Brlit (drvic10k), bootstrap-sortable contributors
 */

/**
 * TinySort is a small script that sorts HTML elements. It sorts by text- or attribute value, or by that of one of it's children.
 * @summary A nodeElement sorting script.
 * @version 2.3.6
 * @license MIT
 * @author Ron Valstar <ron@ronvalstar.nl>
 * @copyright Ron Valstar <ron@ronvalstar.nl>
 * @namespace tinysort
 */
!function (e, t) { "use strict"; function r() { return t } "function" == typeof define && define.amd ? define("tinysort", r) : e.tinysort = t }(this, function () { "use strict"; function e(e, n) { function s() { 0 === arguments.length ? v({}) : t(arguments, function (e) { v(x(e) ? { selector: e } : e) }), d = $.length } function v(e) { var t = !!e.selector, n = t && ":" === e.selector[0], o = r(e || {}, m); $.push(r({ hasSelector: t, hasAttr: !(o.attr === l || "" === o.attr), hasData: o.data !== l, hasFilter: n, sortReturnNumber: "asc" === o.order ? 1 : -1 }, o)) } function S() { t(e, function (e, t) { M ? M !== e.parentNode && (k = !1) : M = e.parentNode; var r = $[0], n = r.hasFilter, o = r.selector, a = !o || n && e.matchesSelector(o) || o && e.querySelector(o), l = a ? R : V, s = { elm: e, pos: t, posn: l.length }; B.push(s), l.push(s) }), D = R.slice(0) } function y(e, t, r) { for (var n = r(e.toString()), o = r(t.toString()), a = 0; n[a] && o[a]; a++) if (n[a] !== o[a]) { var l = Number(n[a]), s = Number(o[a]); return l == n[a] && s == o[a] ? l - s : n[a] > o[a] ? 1 : -1 } return n.length - o.length } function N(e) { for (var t, r, n = [], o = 0, a = -1, l = 0; t = (r = e.charAt(o++)).charCodeAt(0) ;) { var s = 46 == t || t >= 48 && 57 >= t; s !== l && (n[++a] = "", l = s), n[a] += r } return n } function C(e, r) { var n = 0; for (0 !== p && (p = 0) ; 0 === n && d > p;) { var l = $[p], s = l.ignoreDashes ? f : u; if (t(h, function (e) { var t = e.prepare; t && t(l) }), l.sortFunction) n = l.sortFunction(e, r); else if ("rand" == l.order) n = Math.random() < .5 ? 1 : -1; else { var c = a, g = w(e, l), m = w(r, l), v = "" === g || g === o, S = "" === m || m === o; if (g === m) n = 0; else if (l.emptyEnd && (v || S)) n = v && S ? 0 : v ? 1 : -1; else { if (!l.forceStrings) { var C = x(g) ? g && g.match(s) : a, b = x(m) ? m && m.match(s) : a; if (C && b) { var A = g.substr(0, g.length - C[0].length), F = m.substr(0, m.length - b[0].length); A == F && (c = !a, g = i(C[0]), m = i(b[0])) } } n = g === o || m === o ? 0 : l.natural && (isNaN(g) || isNaN(m)) ? y(g, m, N) : m > g ? -1 : g > m ? 1 : 0 } } t(h, function (e) { var t = e.sort; t && (n = t(l, c, g, m, n)) }), n *= l.sortReturnNumber, 0 === n && p++ } return 0 === n && (n = e.pos > r.pos ? 1 : -1), n } function b() { var e = R.length === B.length; if (k && e) O ? R.forEach(function (e, t) { e.elm.style.order = t }) : M ? M.appendChild(A()) : console.warn("parentNode has been removed"); else { var t = $[0], r = t.place, n = "org" === r, o = "start" === r, a = "end" === r, l = "first" === r, s = "last" === r; if (n) R.forEach(F), R.forEach(function (e, t) { E(D[t], e.elm) }); else if (o || a) { var c = D[o ? 0 : D.length - 1], i = c && c.elm.parentNode, u = i && (o && i.firstChild || i.lastChild); u && (u !== c.elm && (c = { elm: u }), F(c), a && i.appendChild(c.ghost), E(c, A())) } else if (l || s) { var f = D[l ? 0 : D.length - 1]; E(F(f), A()) } } } function A() { return R.forEach(function (e) { q.appendChild(e.elm) }), q } function F(e) { var t = e.elm, r = c.createElement("div"); return e.ghost = r, t.parentNode.insertBefore(r, t), e } function E(e, t) { var r = e.ghost, n = r.parentNode; n.insertBefore(t, r), n.removeChild(r), delete e.ghost } function w(e, t) { var r, n = e.elm; return t.selector && (t.hasFilter ? n.matchesSelector(t.selector) || (n = l) : n = n.querySelector(t.selector)), t.hasAttr ? r = n.getAttribute(t.attr) : t.useVal ? r = n.value || n.getAttribute("value") : t.hasData ? r = n.getAttribute("data-" + t.data) : n && (r = n.textContent), x(r) && (t.cases || (r = r.toLowerCase()), r = r.replace(/\s+/g, " ")), null === r && (r = g), r } function x(e) { return "string" == typeof e } x(e) && (e = c.querySelectorAll(e)), 0 === e.length && console.warn("No elements to sort"); var D, M, q = c.createDocumentFragment(), B = [], R = [], V = [], $ = [], k = !0, z = e.length && e[0].parentNode, L = z.rootNode !== document, O = e.length && (n === o || n.useFlex !== !1) && !L && -1 !== getComputedStyle(z, null).display.indexOf("flex"); return s.apply(l, Array.prototype.slice.call(arguments, 1)), S(), R.sort(C), b(), R.map(function (e) { return e.elm }) } function t(e, t) { for (var r, n = e.length, o = n; o--;) r = n - o - 1, t(e[r], r) } function r(e, t, r) { for (var n in t) (r || e[n] === o) && (e[n] = t[n]); return e } function n(e, t, r) { h.push({ prepare: e, sort: t, sortBy: r }) } var o, a = !1, l = null, s = window, c = s.document, i = parseFloat, u = /(-?\d+\.?\d*)\s*$/g, f = /(\d+\.?\d*)\s*$/g, h = [], d = 0, p = 0, g = String.fromCharCode(4095), m = { selector: l, order: "asc", attr: l, data: l, useVal: a, place: "org", returns: a, cases: a, natural: a, forceStrings: a, ignoreDashes: a, sortFunction: l, useFlex: a, emptyEnd: a }; return s.Element && function (e) { e.matchesSelector = e.matchesSelector || e.mozMatchesSelector || e.msMatchesSelector || e.oMatchesSelector || e.webkitMatchesSelector || function (e) { for (var t = this, r = (t.parentNode || t.document).querySelectorAll(e), n = -1; r[++n] && r[n] != t;); return !!r[n] } }(Element.prototype), r(n, { loop: t }), r(e, { plugin: n, defaults: m }) }());

(function (global, factory) {
	if (typeof define === 'function' && define.amd) {
		define(['jquery', 'tinysort', 'moment'], factory);
	} else {
		factory(global.jQuery, global.tinysort, global.moment || undefined);
	}
})(this, function ($, tinysort, moment) {

    var $document = $(document),
        signClass,
        sortEngine,
        emptyEnd;

    $.bootstrapSortable = function (options) {
        if (options == undefined) {
            initialize({});
        }
        else if (options.constructor === Boolean) {
            initialize({ applyLast: options });
        }
        else if (options.sortingHeader !== undefined) {
            sortByColumn(options.sortingHeader);
        }
        else {
            initialize(options);
        }
    };

    function initialize(options) {
        // Check if moment.js is available
        var momentJsAvailable = (typeof moment !== 'undefined');

        // Set class based on sign parameter
        signClass = !options.sign ? "arrow" : options.sign;

        // Set sorting algorithm
        if (options.customSort == 'default')
            options.customSort = defaultSortEngine;
        sortEngine = options.customSort || sortEngine || defaultSortEngine;

        emptyEnd = options.emptyEnd;

        // Set attributes needed for sorting
        $('table.sortable').each(function () {
            var $this = $(this);
            var applyLast = (options.applyLast === true);
            $this.find('span.sign').remove();

            // Add placeholder cells for colspans
            $this.find('> thead [colspan]').each(function () {
                var colspan = parseFloat($(this).attr('colspan'));
                for (var i = 1; i < colspan; i++) {
                    $(this).after('<th class="colspan-compensate">');
                }
            });

            // Add placeholder cells for rowspans
            $this.find('> thead [rowspan]').each(function () {
                var $cell = $(this);
                var rowspan = parseFloat($cell.attr('rowspan'));
                for (var i = 1; i < rowspan; i++) {
                    var parentRow = $cell.parent('tr');
                    var nextRow = parentRow.next('tr');
                    var index = parentRow.children().index($cell);
                    nextRow.children().eq(index).before('<th class="rowspan-compensate">');
                }
            });

            // Set indexes to header cells
            $this.find('> thead tr').each(function (rowIndex) {
                $(this).find('th').each(function (columnIndex) {
                    var $header = $(this);
                    $header.addClass('nosort').removeClass('up down');
                    $header.attr('data-sortcolumn', columnIndex);
                    $header.attr('data-sortkey', columnIndex + '-' + rowIndex);
                });
            });

            // Cleanup placeholder cells
            $this.find('> thead .rowspan-compensate, .colspan-compensate').remove();

            // Initialize sorting values specified in header
            $this.find('th').each(function () {
                var $header = $(this);
                if ($header.attr('data-dateformat') !== undefined && momentJsAvailable) {
                    var colNumber = parseFloat($header.attr('data-sortcolumn'));
                    $this.find('td:nth-child(' + (colNumber + 1) + ')').each(function () {
                        var $cell = $(this);
                        $cell.attr('data-value', moment($cell.text(), $header.attr('data-dateformat')).format('YYYY/MM/DD/HH/mm/ss'));
                    });
                }
                else if ($header.attr('data-valueprovider') !== undefined) {
                    var colNumber = parseFloat($header.attr('data-sortcolumn'));
                    $this.find('td:nth-child(' + (colNumber + 1) + ')').each(function () {
                        var $cell = $(this);
                        $cell.attr('data-value', new RegExp($header.attr('data-valueprovider')).exec($cell.text())[0]);
                    });
                }
            });

            // Initialize sorting values
            $this.find('td').each(function () {
                var $cell = $(this);
                if ($cell.attr('data-dateformat') !== undefined && momentJsAvailable) {
                    $cell.attr('data-value', moment($cell.text(), $cell.attr('data-dateformat')).format('YYYY/MM/DD/HH/mm/ss'));
                }
                else if ($cell.attr('data-valueprovider') !== undefined) {
                    $cell.attr('data-value', new RegExp($cell.attr('data-valueprovider')).exec($cell.text())[0]);
                }
                else {
                    $cell.attr('data-value') === undefined && $cell.attr('data-value', $cell.text());
                }
            });

            var context = lookupSortContext($this),
                bsSort = context.bsSort;

            $this.find('> thead th[data-defaultsort!="disabled"]').each(function (index) {
                var $header = $(this);
                var $sortTable = $header.closest('table.sortable');
                $header.data('sortTable', $sortTable);
                var sortKey = $header.attr('data-sortkey');
                var thisLastSort = applyLast ? context.lastSort : -1;
                bsSort[sortKey] = applyLast ? bsSort[sortKey] : $header.attr('data-defaultsort');
                if (bsSort[sortKey] !== undefined && (applyLast === (sortKey === thisLastSort))) {
                    bsSort[sortKey] = bsSort[sortKey] === 'asc' ? 'desc' : 'asc';
                    doSort($header, $sortTable);
                }
            });
        });
    }

    // Add click event to table header
    $document.on('click', 'table.sortable>thead th[data-defaultsort!="disabled"]', function (e) {
        sortByColumn(this);
    });

    // element is the header of the column to sort (the clicked header)
    function sortByColumn(element) {
        var $this = $(element), $table = $this.data('sortTable') || $this.closest('table.sortable');
        doSort($this, $table);
    }

    // Look up sorting data appropriate for the specified table (jQuery element).
    // This allows multiple tables on one page without collisions.
    function lookupSortContext($table) {
        var context = $table.data("bootstrap-sortable-context");
        if (context === undefined) {
            context = { bsSort: [], lastSort: undefined };
            $table.find('> thead th[data-defaultsort!="disabled"]').each(function (index) {
                var $this = $(this);
                var sortKey = $this.attr('data-sortkey');
                context.bsSort[sortKey] = $this.attr('data-defaultsort');
                if (context.bsSort[sortKey] !== undefined) {
                    context.lastSort = sortKey;
                }
            });
            $table.data("bootstrap-sortable-context", context);
        }
        return context;
    }

    function defaultSortEngine(rows, sortingParams) {
        tinysort(rows, sortingParams);
    }

    // Sorting mechanism separated
    function doSort($this, $table) {
        $table.trigger('before-sort');

        var sortColumn = parseFloat($this.attr('data-sortcolumn')),
            context = lookupSortContext($table),
            bsSort = context.bsSort;

        var colspan = $this.attr('colspan');
        if (colspan) {
            var mainSort = parseFloat($this.data('mainsort')) || 0;
            var rowIndex = parseFloat($this.data('sortkey').split('-').pop());

            // If there is one more row in header, delve deeper
            if ($table.find('> thead tr').length - 1 > rowIndex) {
                doSort($table.find('[data-sortkey="' + (sortColumn + mainSort) + '-' + (rowIndex + 1) + '"]'), $table);
                return;
            }
            // Otherwise, just adjust the sortColumn
            sortColumn = sortColumn + mainSort;
        }

        var localSignClass = $this.attr('data-defaultsign') || signClass;

        // update arrow icon
        $table.find('> thead th').each(function () {
            $(this).removeClass('up').removeClass('down').addClass('nosort');
        });

        if ($.browser.mozilla) {
            var moz_arrow = $table.find('> thead div.mozilla');
            if (moz_arrow !== undefined) {
                moz_arrow.find('.sign').remove();
                moz_arrow.parent().html(moz_arrow.html());
            }
            $this.wrapInner('<div class="mozilla"></div>');
            $this.children().eq(0).append('<span class="sign ' + localSignClass + '"></span>');
        }
        else {
            $table.find('> thead span.sign').remove();
            $this.append('<span class="sign ' + localSignClass + '"></span>');
        }

        // sort direction
        var sortKey = $this.attr('data-sortkey');
        var initialDirection = $this.attr('data-firstsort') !== 'desc' ? 'desc' : 'asc';

        var newDirection = (bsSort[sortKey] || initialDirection);
        if (context.lastSort === sortKey || bsSort[sortKey] === undefined) {
            newDirection = newDirection === 'asc' ? 'desc' : 'asc';
        }
        bsSort[sortKey] = newDirection;
        context.lastSort = sortKey;

        if (bsSort[sortKey] === 'desc') {
            $this.find('span.sign').addClass('up');
            $this.addClass('up').removeClass('down nosort');
        } else {
            $this.addClass('down').removeClass('up nosort');
        }

        // remove rows that should not be sorted
        var rows = $table.children('tbody').children('tr');
        var fixedRows = [];
        $(rows.filter('[data-disablesort="true"]').get().reverse()).each(function (index, fixedRow) {
            var $fixedRow = $(fixedRow);
            fixedRows.push({ index: rows.index($fixedRow), row: $fixedRow });
            $fixedRow.remove();
        });

        // sort rows
        var rowsToSort = rows.not('[data-disablesort="true"]');
        if (rowsToSort.length != 0) {
            var emptySorting = bsSort[sortKey] === 'asc' ? emptyEnd : false;
            sortEngine(rowsToSort, { emptyEnd: emptySorting, selector: 'td:nth-child(' + (sortColumn + 1) + ')', order: bsSort[sortKey], data: 'value' });
        }

        // add back the fixed rows
        $(fixedRows.reverse()).each(function (index, row) {
            if (row.index === 0) {
                $table.children('tbody').prepend(row.row);
            } else {
                $table.children('tbody').children('tr').eq(row.index - 1).after(row.row);
            }
        });

        // add class to sorted column cells
        $table.find('> tbody > tr > td.sorted,> thead th.sorted').removeClass('sorted');
        rowsToSort.find('td:eq(' + sortColumn + ')').addClass('sorted');
        $this.addClass('sorted');
        $table.trigger('sorted');
    }

    // jQuery 1.9 removed this object
    if (!$.browser) {
        $.browser = { chrome: false, mozilla: false, opera: false, msie: false, safari: false };
        var ua = navigator.userAgent;
        $.each($.browser, function (c) {
            $.browser[c] = ((new RegExp(c, 'i').test(ua))) ? true : false;
            if ($.browser.mozilla && c === 'mozilla') { $.browser.mozilla = ((new RegExp('firefox', 'i').test(ua))) ? true : false; }
            if ($.browser.chrome && c === 'safari') { $.browser.safari = false; }
        });
    }

    // Initialise on DOM ready
    $($.bootstrapSortable);

});
