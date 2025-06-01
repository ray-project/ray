Cypress.on(`window:before:load`, win => {
    cy.stub( win.console, `error`, msg => {
        // Abort the test on any error in console,
        // this is a hack to show full stack trace.
        throw new Error( msg.stack );
    });
});
