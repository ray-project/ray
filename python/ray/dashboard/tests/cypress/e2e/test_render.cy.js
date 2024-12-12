describe("Ray Dashboard Test", () => {
  it("opens a new Ray dashboard", () => {
    cy.visit("localhost:8653");
    cy.contains("Overview");
    cy.contains("Jobs");
    cy.contains("Cluster");
    cy.contains("Logs");
  });
});
