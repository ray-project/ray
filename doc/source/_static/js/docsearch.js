docsearch({
    apiKey: '6c42f30d9669d8e42f6fc92f44028596',
    indexName: 'docs-ray',
    container: '#docsearch',
    appId: 'LBHF0PABBL',
    searchParameters: {
        hitsPerPage: 12,
    },
    autocompleteOptions: {
        autoselect: false,
    },
    handleSelected: function (input, event, suggestion, datasetNumber, context) {
        if (context.selectionMethod === 'click') {
            input.setVal('');
            const windowReference = window.open(suggestion.url, "_self");
            windowReference.focus();
        }
    }
});
