anyscaleButton = document.createElement("button");
anyscaleButton.setAttribute("class", "try-anyscale");
anyscaleButton.setAttribute("onclick", "window.open('https://www.anyscale.com','_blank');");
// anyscaleButton.setAttribute("onclick", "location.href='https://www.anyscale.com/signup'");
anyscaleButton.innerHTML = '<span>Learn about managed Ray on Anyscale</span><i class="fas fa-chevron-right" aria-hidden="true" title="Hide"></i>'
document.getElementsByClassName("topbar-main")[0].prepend(anyscaleButton)