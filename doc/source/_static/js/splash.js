function animateTabs() {
  const tabs = Array.from(document.getElementById("v-pills-tab").children)
  const contentTabs = Array.from(document.getElementById("v-pills-tabContent").children)

  tabs.forEach((item, index) => {
    item.onclick = () => {
      tabs.forEach((tab, i) => {
        if (i === index) {
          item.classList.add('active')
        } else {
          tab.classList.remove('active')
        }
      })
      contentTabs.forEach((tab, i) => {
        if (i === index) {
          tab.classList.add('active', 'show')
        } else {
          tab.classList.remove('active', 'show')
        }
      })
    }
  })
}

document.addEventListener("DOMContentLoaded", animateTabs)
document.addEventListener("DOMContentLoaded", () => hljs.highlightAll())
