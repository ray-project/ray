let useTrain = false;
let useTune = false;
let usePredict = false;
let useData= false;
let anyChecked = false;
let multipleComponents = false;


function toggleCheck(button) {

    if (button.classList.contains('btn-primary')) {
        button.classList.replace('btn-primary', 'btn-outline-primary');
    } else {
        button.classList.replace('btn-outline-primary', 'btn-primary');
    }
}

function toggleVisibility(section) {
    if (section.style.cssText.includes('none')) {
        section.style.cssText = 'display: flex !important'
    } else {
        section.style.cssText = 'display: none !important'
    }

    const generateButton = document.getElementById('generateButton');
    const instructions = document.getElementById('instructions');
    const airSelection = document.getElementById('airSelection');
    const airInstructions = document.getElementById('airInstructions');

    anyChecked = useTrain || useTune || usePredict || useData;
    multipleComponents = useTrain + useTune + usePredict + useData > 1;

    if (anyChecked) {
        generateButton.style.cssText = 'display: flex !important'
        instructions.style.cssText = 'display: flex !important'
    } else {
        generateButton.style.cssText = 'display: none !important'
        instructions.style.cssText = 'display: none !important'
    }

    if (multipleComponents) {
        airSelection.style.cssText = 'display: flex !important'
        airInstructions.style.cssText = 'display: flex !important'

    } else {
        airSelection.style.cssText = 'display: none !important'
        airInstructions.style.cssText = 'display: none !important'
    }
}

window.addEventListener("load", () => {

    const basicSelection = document.getElementById("basicSelection");
    if (basicSelection) {

        const data = document.getElementById("dataWizard");
        const dataSelection = document.getElementById("dataSelection");

        data.addEventListener("click", () => {
            useData = ! useData;
            toggleCheck(data);
            toggleVisibility(dataSelection);
        });

        const train = document.getElementById("trainWizard");
        const trainSelection = document.getElementById("trainSelection");

        train.addEventListener("click", () => {
            useTrain = ! useTrain;
            toggleCheck(train);
            toggleVisibility(trainSelection);
        });

        const tune = document.getElementById("tuneWizard");
        const tuneSelection = document.getElementById("tuneSelection");

        tune.addEventListener("click", () => {
            useTune = ! useTune;
            toggleCheck(tune);
            toggleVisibility(tuneSelection);
        });

        const pred = document.getElementById("predWizard");
        const predSelection = document.getElementById("predSelection");

        pred.addEventListener("click", () => {
            usePredict = ! usePredict;
            toggleCheck(pred);
            toggleVisibility(predSelection);
        });

    }

    const submitButton = document.getElementById('generateButton');
    submitButton.addEventListener('click', function (event) {
          const pageUrl = window.location.href
        const res = pageUrl.split("/");

        let baseUrl = "";
        if (res[2] === "docs.ray.io") {
            baseUrl = "https://docs.ray.io/en/" +  res[4];
        }

        // TODO Select the right example here:
        let example = baseUrl + "/wizard/tune_test.html";

        fetch(example)
          .then(response => response.text())
          .then(html => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const code = doc.getElementsByClassName("highlight-default")[0];

            const wizardCode = document.getElementById("wizardCode");
            wizardCode.innerHTML = code.innerHTML;
          })
          .catch(error => console.log(error));
        });



});



