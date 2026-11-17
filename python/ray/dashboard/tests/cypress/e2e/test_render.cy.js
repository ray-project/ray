describe("Ray Dashboard Test", () => {
  it("opens a new Ray dashboard", () => {
    // Logs in the way the dashboard's token prompt does, which sets the auth cookie.
    cy.request({
      method: "POST",
      url: "localhost:8653/api/authenticate",
      headers: { Authorization: `Bearer ${Cypress.env("RAY_AUTH_TOKEN")}` },
    });
    cy.visit("localhost:8653");
    cy.contains("Overview");
    cy.contains("Jobs");
    cy.contains("Cluster");
    cy.contains("Logs");
  });
});
