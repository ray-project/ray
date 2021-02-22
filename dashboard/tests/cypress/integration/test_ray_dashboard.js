describe('Ray Dashboard Test', () => {
    it('opens a new Ray dashboard', () => {
        cy.exec('ray stop').its('code').should('eq', 0)
        cy.exec('ray start --head --dashboard-port=8653').its('code').should('eq', 0)
        cy.exec('python3 -c "import ray ; ray_obj = ray.init() ; print(ray_obj[\'webui_url\']) "').then(
            ($url) => {
            cy.visit($url['stdout'])
            // once this works, click on a button and ensure that works
        })})
  })